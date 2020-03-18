use crate as nats;

use async_std::task;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};

use std::io;
use std::time::Duration;

use prost::{
    Message as _,
};

pub mod pb;
use pb::{
    MsgProto as Message,
};

pub trait ConnectionState: private::Sealed {}
mod private {
    pub trait Sealed {}
    impl Sealed for super::NotConnected {}
    impl Sealed for super::Connected {}
}

impl ConnectionState for NotConnected {}
impl ConnectionState for Connected {}

#[doc(hidden)]
pub struct NotConnected{}


pub struct Subscription {
    sub: nats::Subscription,
}

static DEFAULT_DISCOVER_PREFIX: &str = "_STAN.discover";
static DEFAULT_ACK_PREFIX: &str = "_STAN.acks";

// // helper
// fn prost_decode<T>(v: Vec<u8>)

// derive this noise
impl Subscription {
    pub fn next(&self) -> Option<Message> {
        self.sub.recv
            .iter()
            .next()
            .map(|msg| pb::MsgProto::decode(io::Cursor::new(msg.data))
                 .expect("msg in streaming inbox did not contain valid MsgProto"))
    }
}

use std::collections::HashMap;
#[doc(hidden)]
pub struct Connected {
    // its weird these aren't in the subscription
    heartbeat_inbox: String,
    heartbeat_sub: nats::Subscription,

    ping_inbox: String,
    ping_sub: nats::Subscription,

    // subs: HashMap<usize, >
}

#[derive(Debug,Default)]
pub struct Options {
		// NatsURL:            DefaultNatsURL,
		// ConnectTimeout:     DefaultConnectWait,
		// AckTimeout:         DefaultAckWait,
		// DiscoverPrefix:     DefaultDiscoverPrefix,
		// MaxPubAcksInflight: DefaultMaxPubAcksInflight,
		// PingIterval:        DefaultPingInterval,
		// PingMaxOut:         DefaultPingMaxOut,

    cluster_id: String,
    client_id: String,

    // :todo Default this to correct
    discover_prefix: String,

    // :todo Add these options, and add with_* to Connect<NotConnected>
    //
	// ConnectTimeout time.Duration

	// AckTimeout is how long to wait when a message is published for an ACK from
	// the cluster. If the library does not receive an ACK after this timeout,
	// the Publish() call (or the AckHandler) will return ErrTimeout.
	// AckTimeout time.Duration

	// DiscoverPrefix is the prefix connect requests are sent to for this cluster.
	// The default is "_STAN.discover".
	// DiscoverPrefix string

	// MaxPubAcksInflight specifies how many messages can be published without
	// getting ACKs back from the cluster before the Publish() or PublishAsync()
	// calls block.
	// MaxPubAcksInflight int

	// PingInterval is the interval at which client sends PINGs to the server
	// to detect the loss of a connection.
	// PingIterval int
    ping_interval: i32,

	// PingMaxOut specifies the maximum number of PINGs without a corresponding
	// PONG before declaring the connection permanently lost.
	// PingMaxOut int
    ping_max_out: i32,
}

// underlying conn not-Connected => T not-Connected
#[derive(Debug)]
pub struct Connection<S: nats::ConnectionState, T: ConnectionState> {
    pub conn: nats::Connection<S>,

    // :consider nest this struct or flatten to direct fields?
    options: Options,
    state: T,
}

impl Connection<nats::Connected, NotConnected> {
    // wrap an existing nats connection
    // :testme can you invoke this as just Connection::from_nats_connection
    pub fn from_nats_connection(conn: nats::Connection<nats::Connected>) -> Self {
        Connection{
            conn: conn,
            options: Options::default(),
            state: NotConnected{},
        }
    }

    // BUILDER these? or params to connect?
    //
    // pub fn with_cluster_id<T: Into<String>>(self, cluster_id: T) -> Self {
    //     self.options.cluster_id = cluster_id.into();
    //     self
    // }

    // pub fn with_client_id<T: Into<String>>(self, client_id: T) -> Self {
    //     self.options.client_id = client_id.into();
    //     self
    // }

    // upgrade bare NATS connection to streaming
    pub fn connect(self, cluster_id: impl AsRef<str>, client_id: impl AsRef<str>) -> io::Result<Connection<nats::Connected, Connected>> {
        // :todo store all  these vars in Connection

        // :check you can have a stan clientid distinct from nats clientid
        // get owned copies
        let cluster_id = cluster_id.as_ref().to_owned();
        let client_id = client_id.as_ref().to_owned();

        let conn_id = nuid::next();
        let publish_nuid = nuid::NUID::new();

        // we respond to heartbeat with empty msg (OK)
        let heartbeat_inbox = self.conn.new_inbox();
        let heartbeat_sub = self.conn.subscribe(&heartbeat_inbox)?;
        let heartbeat_handler = task::spawn(async move {
            let sub = heartbeat_sub;
            loop {
                // unwrap: connection closed
                let msg = sub.next().unwrap();
                msg.respond(&[]).map_err(|e| eprintln!("error in heartbeat respond {}", e));
            }
        });

        // let heartbeat_sub = self.conn.
        //     subscribe(&heartbeat_inbox)?
        //     .with_handler(|msg| msg.respond(&[]))
        //     ;

        // server replies to our ping requests with:
        //
        //   1. empty msg (OK) => reset no ping counter
        //   2. msg with nonempty Error => connection is closed
        //
        //  :consider create Handler trait instead of shoving
        //  everything into an opaque function
        let ping_inbox = self.conn.new_inbox();
        let ping_sub = self.conn.subscribe(&ping_inbox)?;
        // ping handler set up after connection response

            // .with_handler(move |msg| {
            //     if msg.data.is_empty() {
            //         // reset ping counter
            //         return
            //     }

            //     // otherwise
            //     // increment ping counter
            //     // fail on exceed ping
            // })
            // ;

        let discover_subject = format!("{}.{}",
                                       self.options.discover_prefix,
                                       cluster_id,
                                       );


        let connect_request = pb::ConnectRequest{
            client_id: client_id.clone(),
            heartbeat_inbox: heartbeat_inbox.clone(),
            protocol: 1,
            conn_id: conn_id.into_bytes(),
            ping_interval: self.options.ping_interval,
            ping_max_out: self.options.ping_max_out,
        };

        let connect_request_encoded = {
            let mut b = Vec::new();
            connect_request.encode(&mut b);
            b
        };

        let connect_response = {
            let buf = self.conn.request(&discover_subject,
                                  &connect_request_encoded,
                                  )?.data;

            pb::ConnectResponse::decode(io::Cursor::new(buf))?
        };

        unimplemented!();

        // Capture cluster configuration endpoints to publish and subscribe/unsubscribe.
        // c.pubPrefix = cr.PubPrefix
        // c.subRequests = cr.SubRequests
        // c.unsubRequests = cr.UnsubRequests
        // c.subCloseRequests = cr.SubCloseRequests
        // c.closeRequests = cr.CloseRequests

        // Setup the ACK subscription
        let ack_subject = format!("{}.{}", DEFAULT_ACK_PREFIX, nuid::next());
        let ack_sub = self.conn.subscribe(&ack_subject)?;
        // c.ackSubscription.SetPendingLimits(1024*1024, 32*1024*1024)
        // Setup Publish Acker

        // Create Subscription map
        // let subs = Arc::new(Mutex::new(HashMap<usize, _>::new()));

        // :skip Capture the connection error cb

        // assume we don't have to deal with this for now
        // Protocol v1 ping stuff; let server dictate parameters
        assert!(
            connect_response.protocol >= 1 && connect_response.ping_interval != 0,
            "server looks too old for us",
            );

        let ping_requests = connect_response.ping_requests;

        // in tests, ping interval is allowed to be negative!
        assert!(connect_response.ping_interval > 0);
        let ping_interval = Duration::from_secs(connect_response.ping_interval as u64);

        // precooked ping msg
        let ping_bytes = {
            let mut buf = Vec::new();
            pb::Ping{conn_id: conn_id.into_bytes()}.encode(&mut buf)?; // always succeeds
            buf
        };

        // turn on pinger
        let ping_handler = task::spawn(async move {
            let sub = ping_sub;
            let interval = ping_interval;
            let mut ping_out = 0;
            loop {
                ping_out += 1;
                task::sleep(interval);
            }
        });

        // Ok(Connection{
        //     conn: self.conn,
        //     options: self.options,
        //     state: Connected{},
        // })
    }
}

// :todo not sure if we'll need our own Subscription type
// type Subscription = nats::Subscription;

impl Connection<nats::Connected, Connected> {
    pub fn publish(subject: impl Into<String>, msg: impl AsRef<[u8]>) -> io::Result<()> {
        unimplemented!()
    }

    pub fn subscribe() -> io::Result<Subscription> {
        unimplemented!()
    }

    // be graceful and always unsubscribe
    pub fn close(self) -> io::Result<()> {
        unimplemented!()
        // stop pinging
        //
        // unsubscribe hb, ping, ack
        //
        // fail pending pubs
        //
        // send pb::CloseRequests
        //
        // close nats connection
    }
}

