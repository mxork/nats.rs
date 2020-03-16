use std::io;
use crate as nats;

pub mod pb;

pub trait ConnectionState: private::Sealed {}
mod private {
    pub trait Sealed {}
    impl Sealed for super::NotConnected {}
    impl Sealed for super::Connected {}
}

#[doc(hidden)]
pub struct NotConnected{};

#[doc(hidden)]
pub struct Connected{};

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

	// PingMaxOut specifies the maximum number of PINGs without a corresponding
	// PONG before declaring the connection permanently lost.
	// PingMaxOut int
};

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
            state: NotConnected,
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
    pub fn connect(self, cluster_id: impl Into<String>, client_id: impl Into<String>) -> io::Result<Connection<Connected>> {
        // :todo store all  these vars in Connection

        // :check you can have a stan clientid distinct from nats clientid
        // get owned copies
        let cluster_id = cluster_id.into();
        let client_id = client_id.into();

        let conn_id = nuid::next();
        let publish_nuid = nuid::new();

        let hearbeat_inbox = self.conn.new_inbox();
        let hearbeat_sub = self.conn.subscribe(&hearbeat_inbox)?;
        // :todo impl process heartbeat

        let ping_inbox = self.conn.new_inbox();
        let ping_sub = self.conn.subscribe(&ping_inbox);
        // :todo impl process ping

        let discover_subject = format!("{}.{}",
                                       self.options.discover_prefix,
                                       cluster_id,
                                       );


        let connect_request: pb::ConnectRequest{
            client_id: client_id.clone(),
            heartbeat_inbox: hearbeat_inbox.clone(),
            protocol: 1.
            conn_id: conn_id,
            ping_interval: self.options.ping_interval,
            ping_max_out: self.options.ping_max_out,
        };

        let connect_request_encoded = {
            let mut b = Vec::new();
            connect_request.encode(&mut b);
            b
        };

        let connect_response =
            pb::ConnectResponse::decode(
                self.conn.request(&discover_subject,
                                  &connect_request_encoded,
                                  )?
                );

        unimplemented!();

        // Capture cluster configuration endpoints to publish and subscribe/unsubscribe.
        // c.pubPrefix = cr.PubPrefix
        // c.subRequests = cr.SubRequests
        // c.unsubRequests = cr.UnsubRequests
        // c.subCloseRequests = cr.SubCloseRequests
        // c.closeRequests = cr.CloseRequests

        // Setup the ACK subscription
        // Create Subscription map
        // Capture the connection error cb
        // Protocol v1 ping stuff; let server dictate parameters

        Ok(Connection{
            conn: self.conn,
            options: self.options,
            state: Connected,
        })
    }
}

// :todo not sure if we'll need our own Subscription type
type Subscription = nats::Subscription;

impl Connection<nats::Connected, Connected> {
    pub fn publish(subject: impl Into<String>, msg: AsRef<[u8]>) -> io::Result<()> {
        unimplemented!()
    }

    pub fn subscribe() -> io::Result<Subscription> {
        unimplemented!()
    }
}

