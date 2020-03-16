//! A Rust client for the NATS.io ecosystem.
//!
//! <code>git clone https://github.com/nats-io/nats.rs</code>
//!
//! NATS.io is a simple, secure and high performance open source messaging system for cloud native applications,
//! IoT messaging, and microservices architectures.
//!
//! For more information see [https://nats.io/].
//!
//! [https://nats.io/]: https://nats.io/
//!
use std::collections::{HashMap, VecDeque};
use std::io::{self, BufReader, BufWriter, Error, ErrorKind, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{fmt, str, thread};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use serde::{Deserialize, Serialize};

#[cfg(feature = "streaming")]
pub mod streaming;

mod parser;

#[deny(unsafe_code)]

const VERSION: &str = "0.0.1";
const LANG: &str = "rust";

#[doc(hidden)]
pub trait ConnectionState: private::Sealed {}
mod private {
    pub trait Sealed {}
    impl Sealed for super::NotConnected {}
    impl Sealed for super::Authenticated {}
    impl Sealed for super::Connected {}
    impl Sealed for dyn super::OptionState {}
}

impl ConnectionState for NotConnected {}
impl ConnectionState for Authenticated {}
impl ConnectionState for Connected {}

#[derive(Debug)]
pub struct Connection<S: ConnectionState> {
    options: Options,
    state: S,
}

#[doc(hidden)]
pub trait OptionState {}
impl OptionState for NotConnected {}
impl OptionState for Authenticated {}

// General Options
impl<S> Connection<S>
where
    S: OptionState + ConnectionState,
{
    /// Add a name option for the unconnected connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Connection::new()
    ///     .with_name("My App")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_string());
        self
    }

    /// Select option to not deliver messages that we have published.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Connection::new()
    ///     .no_echo()
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn no_echo(mut self) -> Self {
        self.options.no_echo = true;
        self
    }
}

#[derive(Debug, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Closed,
    Disconnected,
    Reconnecting,
}

#[derive(Debug)]
pub(crate) struct Outbound {
    writer: BufWriter<TcpStream>,
    flusher: Option<thread::JoinHandle<()>>,
    should_flush: bool,
    in_flush: bool,
    closed: bool,
}

impl Outbound {
    #[inline(always)]
    fn write_response(&mut self, subj: &str, msgb: &[u8]) -> io::Result<()> {
        write!(self.writer, "PUB {} {}\r\n", subj, msgb.len())?;
        self.writer.write(msgb)?;
        self.writer.write(b"\r\n")?;
        if self.should_flush && !self.in_flush {
            self.kick_flusher();
        }
        Ok(())
    }

    #[inline(always)]
    fn kick_flusher(&self) {
        if let Some(flusher) = &self.flusher {
            flusher.thread().unpark();
        }
    }
}

#[doc(hidden)]
pub struct NotConnected;
#[doc(hidden)]
pub struct Authenticated;

#[derive(Debug)]
#[doc(hidden)]
pub struct Connected {
    id: String,
    status: ConnectionStatus,
    stream: TcpStream,
    info: ServerInfo,
    sid: AtomicUsize,
    subs: Arc<RwLock<HashMap<usize, Sender<Message>>>>,
    pongs: Arc<Mutex<VecDeque<Sender<bool>>>>,
    writer: Arc<Mutex<Outbound>>,
    reader: Option<thread::JoinHandle<()>>,
}

#[derive(Serialize, Clone, Debug)]
enum AuthStyle {
    //    Credentials(String, String),
    Token(String),
    UserPass(String, String),
    None,
}

#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct Options {
    auth: AuthStyle,
    name: Option<String>,
    no_echo: bool,
}

/// Connect to a NATS server at the given url.
///
/// # Example
/// ```
/// # fn main() -> std::io::Result<()> {
/// let nc = nats::connect("demo.nats.io")?;
/// # Ok(())
/// # }
/// ```
pub fn connect(nats_url: &str) -> io::Result<Connection<Connected>> {
    Connection::new().connect(nats_url)
}

impl Connection<NotConnected> {
    /// Create a new NATS connection. This will not be a connected connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Connection::new().connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new() -> Connection<NotConnected> {
        Connection {
            state: NotConnected {},
            options: Options {
                auth: AuthStyle::None,
                name: None,
                no_echo: false,
            },
        }
    }

    /// Authenticate this NATS connection with a token.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Connection::new()
    ///     .with_token("t0k3n!")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_token(self, token: &str) -> Connection<Authenticated> {
        let mut opts = self.options.clone();
        opts.auth = AuthStyle::Token(token.to_string());
        Connection {
            state: Authenticated {},
            options: opts,
        }
    }

    /// Authenticate this NATS connection with a username and poassword.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Connection::new()
    ///     .with_user_pass("derek", "s3cr3t!")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_user_pass(self, user: &str, password: &str) -> Connection<Authenticated> {
        let mut opts = self.options.clone();
        opts.auth = AuthStyle::UserPass(user.to_string(), password.to_string());
        Connection {
            state: Authenticated {},
            options: opts,
        }
    }

    #[doc(hidden)]
    pub fn connect(self, nats_url: &str) -> io::Result<Connection<Connected>> {
        let conn = Connection {
            state: Authenticated {},
            options: self.options.clone(),
        };
        let conn = conn.connect(nats_url)?;
        Ok(conn)
    }
}

impl Connection<Authenticated> {
    fn check_port(&self, nats_url: &str) -> String {
        match nats_url.parse::<SocketAddr>() {
            Ok(_) => nats_url.to_string(),
            Err(_) => match nats_url.find(':') {
                Some(_) => nats_url.to_string(),
                None => format!("{}:4222", nats_url),
            },
        }
    }

    /// Connect an unconnected NATS connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Connection::new().connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect(self, nats_url: &str) -> io::Result<Connection<Connected>> {
        let connect_url = self.check_port(nats_url);
        let stream = TcpStream::connect(connect_url)?;

        let mut reader = BufReader::with_capacity(64 * 1024, stream.try_clone()?);
        let server_info = parser::expect_info(&mut reader)?;

        // TODO(dlc) - Fix, but for now at least signal properly.
        if server_info.tls_required {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "TLS currently not supported",
            ));
        }

        let mut n = nuid::NUID::new();

        let mut conn = Connection {
            state: Connected {
                id: n.next(),
                status: ConnectionStatus::Connecting,
                stream: stream.try_clone()?,
                info: server_info,
                sid: AtomicUsize::new(1),
                subs: Arc::new(RwLock::new(HashMap::new())),
                pongs: Arc::new(Mutex::new(VecDeque::new())),
                writer: Arc::new(Mutex::new(Outbound {
                    writer: BufWriter::with_capacity(64 * 1024, stream.try_clone()?),
                    flusher: None,
                    should_flush: true,
                    in_flush: false,
                    closed: false,
                })),
                reader: None,
            },
            options: self.options.clone(),
        };
        conn.send_connect(&mut reader)?;
        conn.state.status = ConnectionStatus::Connected;

        // Setup the state we will move to the readloop thread
        let mut state = parser::ReadLoopState {
            reader: reader,
            writer: conn.state.writer.clone(),
            subs: conn.state.subs.clone(),
            pongs: conn.state.pongs.clone(),
        };

        let read_loop = thread::spawn(move || {
            // FIXME(dlc) - Capture?
            if let Err(_) = parser::read_loop(&mut state) {
                return;
            }
        });
        conn.state.reader = Some(read_loop);

        let usec = Duration::new(0, 1_000);
        let wait: [Duration; 5] = [10 * usec, 100 * usec, 500 * usec, 1000 * usec, 5000 * usec];
        let wbuf = conn.state.writer.clone();

        let flusher_loop = thread::spawn(move || loop {
            thread::park();
            let start_len = start_flush_cycle(&wbuf);
            thread::yield_now();
            let mut cur_len = wbuf.lock().unwrap().writer.buffer().len();
            if cur_len != start_len {
                for d in wait.iter() {
                    thread::sleep(*d);
                    {
                        let w = wbuf.lock().unwrap();
                        cur_len = w.writer.buffer().len();
                        if cur_len == 0 || cur_len == start_len || w.closed {
                            break;
                        }
                    }
                }
            }
            let mut w = wbuf.lock().unwrap();
            w.in_flush = false;
            if cur_len > 0 {
                if let Err(_) = w.writer.flush() {
                    break;
                }
            }
            if w.closed {
                break;
            }
        });
        conn.state.writer.lock().unwrap().flusher = Some(flusher_loop);

        Ok(conn)
    }
}

fn start_flush_cycle(wbuf: &Arc<Mutex<Outbound>>) -> usize {
    let mut w = wbuf.lock().unwrap();
    w.in_flush = true;
    w.writer.buffer().len()
}

#[derive(Debug)]
pub struct Message {
    pub subject: String,
    pub reply: Option<String>,
    pub data: Vec<u8>,
    pub(crate) writer: Option<Arc<Mutex<Outbound>>>,
}

impl Message {
    /// Respond to a request message.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.subscribe("help.request")?.with_handler(move |m| {
    ///     m.respond("ans=42")?; Ok(())
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn respond(&self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        if let Some(writer) = &self.writer {
            if let Some(reply) = &self.reply {
                writer.lock().unwrap().write_response(reply, msg.as_ref())?;
            }
        } else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "No reply subject available",
            ));
        }
        Ok(())
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut body = format!("[{} bytes]", self.data.len());
        if let Ok(str) = str::from_utf8(&self.data) {
            body = str.to_string();
        }
        if let Some(reply) = &self.reply {
            write!(
                f,
                "Message {{\n  subject: \"{}\",\n  reply: \"{}\",\n  data: \"{}\"\n}}",
                self.subject, reply, body
            )
        } else {
            write!(
                f,
                "Message {{\n  subject: \"{}\",\n  data: \"{}\"\n}}",
                self.subject, body
            )
        }
    }
}

#[derive(Clone, Debug)]
pub struct Subscription {
    sid: usize,
    recv: Receiver<Message>,
    subs: Arc<RwLock<HashMap<usize, Sender<Message>>>>,
    writer: Arc<Mutex<Outbound>>,
    do_unsub: bool,
}

impl Subscription {
    /// Get the next message, or None if the subscription has been unsubscribed or the connection closed.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// # nc.publish("foo", "hello")?;
    /// if let Some(msg) = sub.next() {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn next(&self) -> Option<Message> {
        self.recv.iter().next()
    }

    /// Try to get the next message, or None if no messages are present or if the subscription has been unsubscribed or the connection closed.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// if let Some(msg) = sub.try_next() {
    ///   println!("Received {}", msg);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_next(&self) -> Option<Message> {
        self.recv.try_iter().next()
    }

    /// Get the next message, or a timeout error if no messages are available for timout.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// if let Ok(msg) = sub.next_timeout(std::time::Duration::from_secs(1)) {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_timeout(&self, timeout: Duration) -> Result<Message, RecvTimeoutError> {
        self.recv.recv_timeout(timeout)
    }

    /// Returns a blocking message iterator. Same as calling `iter()`.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.messages() {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn messages(&self) -> impl Iterator<Item = Message> + '_ {
        self.recv.iter()
    }

    /// Returns a blocking message iterator.
    ///
    /// # Example
    /// ```rust,no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.iter() {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = Message> + '_ {
        self.recv.iter()
    }

    /// Returns a non-blocking message iterator.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.try_iter() {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_iter(&self) -> impl Iterator<Item = Message> + '_ {
        self.recv.try_iter()
    }

    /// Returns a blocking message iterator with a time deadline for blocking.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.timeout_iter(std::time::Duration::from_secs(1)) {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn timeout_iter(&self, timeout: Duration) -> impl Iterator<Item = Message> + '_ {
        SubscriptionDeadlineIterator {
            r: self.recv.clone(),
            to: timeout,
        }
    }

    /// Attach a closure to handle messages.
    /// This closure will execute in a separate thread.
    /// The result of this call is a `SubscriptionHandler` which can not be
    /// iterated and must be unsubscribed or closed directly to unregister interest.
    /// A SubscriptionHandler will not unregister interest with the server when `drop(&mut self)` is called.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.subscribe("bar")?.with_handler(move |msg| {
    ///     println!("Received {}", &msg);
    ///     Ok(())
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_handler<F>(mut self, handler: F) -> SubscriptionHandler
    where
        F: Fn(Message) -> io::Result<()> + Sync + Send,
        F: 'static,
    {
        // This will allow us to not have to capture the return. When it is dropped it
        // will not unsubscribe from the server.
        self.do_unsub = false;
        let r = self.recv.clone();
        thread::spawn(move || {
            for m in r.iter() {
                if let Err(e) = handler(m) {
                    // TODO(dlc) - Capture for last error?
                    println!("Error in callback! {:?}", e);
                }
            }
        });
        SubscriptionHandler { sub: self }
    }

    fn unsub(&mut self) -> io::Result<()> {
        self.do_unsub = false;
        self.subs.write().unwrap().remove(&self.sid);
        let w = &mut self.writer.lock().unwrap().writer;
        write!(w, "UNSUB {}\r\n", self.sid)?;
        w.flush()?;
        Ok(())
    }

    /// Unsubscribe a subscription.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?;
    /// sub.unsubscribe()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unsubscribe(mut self) -> io::Result<()> {
        self.unsub()
    }

    /// Close a subscription. Same as `unsubscribe`
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?;
    /// sub.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(mut self) -> io::Result<()> {
        self.unsub()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.do_unsub {
            match self.unsub() {
                Ok(_) => {}
                Err(_) => {}
            }
        }
    }
}

pub struct SubscriptionHandler {
    sub: Subscription,
}

impl SubscriptionHandler {
    /// Unsubscribe a subscription.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?.with_handler(move |msg| {
    ///     println!("Received {}", &msg);
    ///     Ok(())
    /// });
    /// sub.unsubscribe()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unsubscribe(mut self) -> io::Result<()> {
        self.sub.unsub()
    }

    /// Close a subscription. Same as `unsubscribe`
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?.with_handler(move |msg| {
    ///     println!("Received {}", &msg);
    ///     Ok(())
    /// });
    /// sub.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(mut self) -> io::Result<()> {
        self.sub.unsub()
    }
}

#[doc(hidden)]
pub struct SubscriptionDeadlineIterator {
    r: Receiver<Message>,
    to: Duration,
}

impl Iterator for SubscriptionDeadlineIterator {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        match self.r.recv_timeout(self.to) {
            Ok(m) => Some(m),
            _ => None,
        }
    }
}

impl Connection<Connected> {
    /// Create a subscription for the given NATS connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, None)
    }

    /// Create a queue subscription for the given NATS connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.queue_subscribe("foo", "production")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn queue_subscribe(&self, subject: &str, queue: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, Some(queue))
    }

    fn do_subscribe(&self, subject: &str, queue: Option<&str>) -> io::Result<Subscription> {
        let sid = self.state.sid.fetch_add(1, Ordering::Relaxed);
        {
            let w = &mut self.state.writer.lock().unwrap();
            match queue {
                Some(q) => write!(w.writer, "SUB {} {} {}\r\n", subject, q, sid)?,
                None => write!(w.writer, "SUB {} {}\r\n", subject, sid)?,
            }
            if w.should_flush && !w.in_flush {
                w.kick_flusher();
            }
        }
        let (s, r) = crossbeam_channel::unbounded();
        {
            let mut subs = self.state.subs.write().unwrap();
            subs.insert(sid, s);
        }
        // TODO(dlc) - Should we do a flush and check errors?
        Ok(Subscription {
            sid: sid,
            recv: r,
            writer: self.state.writer.clone(),
            subs: self.state.subs.clone(),
            do_unsub: true,
        })
    }

    #[doc(hidden)]
    pub fn batch(&self) {
        self.state.writer.lock().unwrap().should_flush = false;
    }

    #[doc(hidden)]
    pub fn unbatch(&self) {
        self.state.writer.lock().unwrap().should_flush = true;
    }

    #[inline(always)]
    fn write_pub_msg(&self, subj: &str, reply: Option<&str>, msgb: &[u8]) -> io::Result<()> {
        let mut w = self.state.writer.lock().unwrap();
        if let Some(reply) = reply {
            write!(w.writer, "PUB {} {} {}\r\n", subj, reply, msgb.len())?;
        } else {
            write!(w.writer, "PUB {} {}\r\n", subj, msgb.len())?;
        }
        w.writer.write(msgb)?;
        w.writer.write(b"\r\n")?;
        if w.should_flush && !w.in_flush {
            w.kick_flusher();
        }
        Ok(())
    }

    /// Publish a message on the given subject.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.publish("foo", "Hello World!")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        self.write_pub_msg(subject, None, msg.as_ref())
    }

    /// Publish a message on the given subject with a reply subject for responses.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let reply = nc.new_inbox();
    /// let rsub = nc.subscribe(&reply)?;
    /// nc.publish_request("foo", &reply, "Help me!")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish_request(
        &self,
        subject: &str,
        reply: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        self.write_pub_msg(subject, Some(reply), msg.as_ref())
    }

    /// Create a new globally unique inbox which can be used for replies.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let reply = nc.new_inbox();
    /// let rsub = nc.subscribe(&reply)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}.{}", self.state.id, nuid::next())
    }

    /// Publish a message on the given subject as a request and receive the response.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// let resp = nc.request("foo", "Help me?")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_request(subject, &reply, msg)?;
        match sub.next() {
            Some(msg) => Ok(msg),
            None => Err(Error::new(ErrorKind::NotConnected, "No response")),
        }
    }

    /// Publish a message on the given subject as a request and receive the response.
    /// This call will return after the timeout duration if no response is received.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// let resp = nc.request_timeout("foo", "Help me?", std::time::Duration::from_secs(2))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_timeout(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
        timeout: Duration,
    ) -> io::Result<Message> {
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_request(subject, &reply, msg)?;
        match sub.next_timeout(timeout) {
            Ok(msg) => Ok(msg),
            Err(_) => Err(Error::new(ErrorKind::TimedOut, "No response")),
        }
    }

    /// Publish a message on the given subject as a request and allow multiple responses.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// for msg in nc.request_multi("foo", "Help")?.iter().take(1) {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_multi(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Subscription> {
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_request(subject, &reply, msg)?;
        Ok(sub)
    }

    /// Flush a NATS connection by sending a `PING` protocol and waiting for the responding `PONG`.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.flush()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush(&self) -> io::Result<()> {
        // TODO(dlc) - bounded or oneshot?
        self.unbatch();
        let (s, r) = crossbeam_channel::unbounded();
        {
            let mut pongs = self.state.pongs.lock().unwrap();
            pongs.push_back(s);
        }
        self.send_ping()?;
        r.recv().unwrap();
        Ok(())
    }

    fn send_ping(&self) -> io::Result<()> {
        let w = &mut self.state.writer.lock().unwrap().writer;
        w.write(b"PING\r\n")?;
        // Flush in place on pings.
        w.flush()?;
        Ok(())
    }

    /// Close a NATS connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(self) -> io::Result<()> {
        drop(self);
        Ok(())
    }

    fn send_connect(&mut self, reader: &mut BufReader<TcpStream>) -> io::Result<()> {
        let mut connect_op = Connect {
            name: self.options.name.as_ref(),
            pedantic: false,
            verbose: false,
            lang: LANG,
            version: VERSION,
            user: None,
            pass: None,
            auth_token: None,
            echo: !self.options.no_echo,
        };
        match &self.options.auth {
            AuthStyle::UserPass(user, pass) => {
                connect_op.user = Some(user);
                connect_op.pass = Some(pass);
            }
            AuthStyle::Token(token) => connect_op.auth_token = Some(token),
            _ => {}
        }
        let op = format!(
            "CONNECT {}\r\nPING\r\n",
            serde_json::to_string(&connect_op)?
        );
        self.state.stream.write_all(op.as_bytes())?;

        match parser::parse_control_op(reader)? {
            parser::ControlOp::Pong => Ok(()),
            parser::ControlOp::Err(e) => Err(Error::new(ErrorKind::ConnectionRefused, e)),
            _ => Err(Error::new(ErrorKind::ConnectionRefused, "Protocol Error")),
        }
    }
}

impl Connected {
    fn close(&mut self) -> io::Result<()> {
        self.status = ConnectionStatus::Closed;
        self.writer.lock().unwrap().closed = true;
        let flusher = self.writer.lock().unwrap().flusher.take();
        if let Some(ft) = flusher {
            ft.thread().unpark();
            if let Err(_) = ft.join() {}
        }
        // Shutdown socket.
        self.stream.shutdown(Shutdown::Both)?;
        if let Some(rt) = self.reader.take() {
            if let Err(_) = rt.join() {}
        }
        Ok(())
    }
}

impl Drop for Connected {
    fn drop(&mut self) {
        match self.close() {
            _ => {}
        }
    }
}

#[derive(Serialize, Debug)]
struct Connect<'a> {
    #[serde(skip_serializing_if = "empty_or_none")]
    name: Option<&'a String>,
    verbose: bool,
    pedantic: bool,
    #[serde(skip_serializing_if = "if_true")]
    echo: bool,
    lang: &'a str,
    version: &'a str,

    // Authentication
    #[serde(skip_serializing_if = "empty_or_none")]
    user: Option<&'a String>,
    #[serde(skip_serializing_if = "empty_or_none")]
    pass: Option<&'a String>,
    #[serde(skip_serializing_if = "empty_or_none")]
    auth_token: Option<&'a String>,
}

#[inline(always)]
fn if_true(field: &bool) -> bool {
    *field == true
}

#[inline(always)]
fn empty_or_none(field: &Option<&String>) -> bool {
    match field {
        Some(_) => false,
        None => true,
    }
}

#[derive(Deserialize, Debug)]
struct ServerInfo {
    server_id: String,
    server_name: String,
    host: String,
    port: i16,
    version: String,
    #[serde(default = "default_false")]
    auth_required: bool,
    #[serde(default = "default_false")]
    tls_required: bool,
    max_payload: i32,
    proto: i8,
    client_id: u64,
    go: String,
    #[serde(default = "default_empty")]
    nonce: String,
    #[serde(default = "default_no_urls")]
    connect_urls: Vec<String>,
}

#[inline(always)]
fn default_false() -> bool {
    false
}
#[inline(always)]
fn default_empty() -> String {
    "".to_string()
}
#[inline(always)]
fn default_no_urls() -> Vec<String> {
    vec![]
}
