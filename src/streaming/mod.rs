use crate as nats;

use std::thread;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize};
type ArcMut<T> = Arc<Mutex<T>>;
type ArcLock<T> = Arc<RwLock<T>>;

use std::collections::{HashMap as Map};

use std::io;
use std::time::Duration;

use log::{trace};

use crossbeam_channel as channel;

use prost::{
    Message as _,
};

pub mod pb;
use pb::{
    MsgProto as Message,
};

static DEFAULT_DISCOVER_PREFIX: &str = "_STAN.discover";
static DEFAULT_ACK_PREFIX: &str = "_STAN.acks";

pub trait ConnectionState: private::Sealed {}
mod private {
    pub trait Sealed {}
    impl Sealed for super::NotConnected {}
    impl Sealed for super::ConnectedNotStreaming {}
    impl Sealed for super::Connected {}
}

impl ConnectionState for NotConnected {}
impl ConnectionState for ConnectedNotStreaming {}
impl ConnectionState for Connected {}

#[doc(hidden)]
pub struct NotConnected {}

#[doc(hidden)]
pub struct ConnectedNotStreaming {
    nats: nats::Connection<nats::Connected>,
}

#[doc(hidden)]
pub struct Connected {
    shared: ArcLock<SharedConnected>,
}

struct SharedConnected {
    cluster_id: String,
    client_id: String,
    conn_id: String,

    connect_response: pb::ConnectResponse,

    nats: nats::Connection<nats::Connected>,
    subscriptions: Map<String, channel::Sender<Message>>,
}

// underlying conn not-Connected => T not-Connected
#[derive(Debug)]
pub struct Connection<T: ConnectionState> {
    state: T,
}

pub fn from_nats(conn: nats::Connection<nats::Connected>)
    -> Connection<ConnectedNotStreaming>
{
    Connection{
        state: ConnectedNotStreaming{
            nats: conn,
        },
    }
}

impl Connection<ConnectedNotStreaming> {
    // upgrade bare NATS connection to streaming
    pub fn upgrade_to_streaming(self, cluster_id: impl AsRef<str>, client_id: impl AsRef<str>) -> io::Result<Connection<Connected>> {
        let conn_shared = Arc::new(RwLock::new(
                SharedConnected {
                    cluster_id: cluster_id.as_ref().to_owned(),
                    client_id: client_id.as_ref().to_owned(),
                    conn_id: nuid::next(),

                    // will overwrite this on success
                    connect_response: pb::ConnectResponse::default(),

                    nats: self.state.nats,
                    subscriptions: Map::new(),
                }
        ));

        let conn_arc = conn_shared.clone();
        let mut conn_l = conn_arc.write().unwrap();
        let conn = &conn_l.nats;

        // :check you can have a stan clientid distinct from nats clientid
        // get owned copies
        let cluster_id = cluster_id.as_ref().to_owned();
        trace!("using cluster id: {}", cluster_id);

        let client_id = client_id.as_ref().to_owned();
        trace!("using client id: {}", client_id);

        let conn_id = conn_l.conn_id.clone();
        trace!("using connection id: {}", conn_id);

        let publish_nuid = nuid::NUID::new();

        // we respond to heartbeat with empty msg (OK)
        let heartbeat_inbox = conn.new_inbox();
        let heartbeat_sub = conn
            .subscribe(&heartbeat_inbox)?
            .with_handler(|msg| {
                trace!("received heartbeat request.");
                msg.respond(&[])
            });

        let discover_subject = format!("{}.{}",
                                       DEFAULT_DISCOVER_PREFIX,
                                       cluster_id,
                                       );

        let connect_request = pb::ConnectRequest{
            client_id: client_id.clone(),
            heartbeat_inbox: heartbeat_inbox.clone(),
            protocol: 1,
            conn_id: conn_id.clone().into_bytes(),
            ..pb::ConnectRequest::default()
            // :todo(ping)
            // ping_interval: self.options.ping_interval,
            // ping_max_out: self.options.ping_max_out,
        };

        trace!("sending ConnectRequest: {:#?}", connect_request);

        let connect_request_encoded = {
            let mut b = Vec::new();
            connect_request.encode(&mut b);
            b
        };

        let connect_response = {
            let buf = conn.request(&discover_subject,
                                  &connect_request_encoded,
                                  )?.data;

            pb::ConnectResponse::decode(io::Cursor::new(buf))?
        };

        trace!("received ConnectResponse: {:#?}", connect_response);

        // Capture cluster configuration endpoints to publish and subscribe/unsubscribe.
        // c.pubPrefix = cr.PubPrefix
        // c.subRequests = cr.SubRequests
        // c.unsubRequests = cr.UnsubRequests
        // c.subCloseRequests = cr.SubCloseRequests
        // c.closeRequests = cr.CloseRequests

        // Setup the ACK subscription
        // :note stan.go does not allow config of DefaultAck
        let ack_subject = format!("{}.{}", DEFAULT_ACK_PREFIX, nuid::next());
        let ack_sub = conn.subscribe(&ack_subject)?;
        // c.ackSubscription.SetPendingLimits(1024*1024, 32*1024*1024)

        // Create Subscription map
        // let subs = Arc::new(Mutex::new(HashMap<usize, _>::new()));

        assert!(connect_response.protocol >= 1,
                "server looks too old for us. ConnectResponse: {:#?}",
                connect_response);

        // :todo(ping)
        // assume server asked for no pings (default v0.17.0)
        // let ping_requests = connect_response.ping_requests;
        assert!(connect_response.ping_interval == 0);

        drop(conn);
        conn_l.connect_response = connect_response;
        Ok(Connection{
            state: Connected{
                shared: conn_shared,
            },
        })
    }
}

fn encode_or_die<T: prost::Message+Sized>(m: T) -> Vec<u8> {
    let mut b = Vec::new();
    m.encode(&mut b).expect("failed to encode");
    b
}

fn decode_or_die<T: prost::Message+Default>(b: Vec<u8>) -> T {
    T::decode(io::Cursor::new(b)).expect("failed to decode")
}

impl Connection<Connected> {
    pub fn publish(subject: impl Into<String>, msg: impl AsRef<[u8]>) -> io::Result<()> {
        unimplemented!()
    }

    pub fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        let shared = self.state.shared.write().unwrap();

        let inbox = shared.nats.new_inbox();
        let sub = shared.nats.subscribe(&inbox)?;

        let subreq = pb::SubscriptionRequest{
            client_id: shared.client_id.clone(),
            subject: subject.to_owned(),
            inbox: inbox.clone(),
            ack_wait_in_secs: 30, // no default here
            max_in_flight: 1,
            ..pb::SubscriptionRequest::default()
        };

        let subreqcooked = encode_or_die(subreq);
        let reply: pb::SubscriptionResponse = {
            let reply = shared.nats.request(&shared.connect_response.sub_requests, subreqcooked)?;
            decode_or_die(reply.data)
        };

        if !reply.error.is_empty() {
            sub.unsubscribe();
            return Err(io::Error::new(io::ErrorKind::Other, reply.error));
        }

        Ok(Subscription {
            inbox,
            ack_inbox: reply.ack_inbox,
            subject: subject.to_owned(),
            sub,
            shared: self.state.shared.clone(),
        })
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

pub struct Subscription {
    inbox: String,
    ack_inbox: String,
    subject: String,

    sub: nats::Subscription,
    shared: ArcLock<SharedConnected>,
}

// derive this noise
impl Subscription {
    pub fn next(&self) -> Option<Message> {
        trace!("receiving message");
        self.sub.recv
            .iter()
            .next()
            .map(|msg| pb::MsgProto::decode(io::Cursor::new(msg.data))
                 .expect("msg in streaming inbox did not contain valid MsgProto"))
    }

    pub fn next_and_ack(&self) -> io::Result<Option<Message>> {
        // :todo(manual ack)
        let msg = match self.next() {
            None => None,
            Some(msg) => {
                trace!("ack'ing message");
                let ack = self.prepare_ack(&msg);
                self.ack(&self.ack_inbox, ack)?;
                Some(msg)
            },
        };

        Ok(msg)
    }

    fn prepare_ack(&self, msg: &Message) -> Vec<u8> {
        let msg = pb::Ack{subject: msg.subject.to_owned(), sequence: msg.sequence};
        let mut buf = Vec::new();
        msg.encode(&mut buf).expect("failed to encode ack");
        buf
    }

    pub fn ack(&self, ack_subject: &str, raw_ack: Vec<u8>) -> io::Result<()> {
        self.shared.read().unwrap()
            .nats.publish(ack_subject, raw_ack)
    }

    pub fn unsubscribe(&mut self) -> io::Result<()> {
        let shared = self.shared.write().unwrap();
        let req = encode_or_die(pb::UnsubscribeRequest{
            client_id: shared.client_id.clone(),
            subject: self.subject.clone(),
            inbox: self.ack_inbox.clone(),
            ..pb::UnsubscribeRequest::default()
        });

        let reply: pb::SubscriptionResponse = decode_or_die(shared.nats.request(&shared.connect_response.unsub_requests, req)?.data);

        if !reply.error.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, reply.error));
        };

        Ok(())
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}
