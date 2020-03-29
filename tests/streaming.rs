use nats;
use nats::streaming as stan;

use log;
use simple_logger;

#[test]
fn main() {
    simple_logger::init_with_level(log::Level::Trace).unwrap();

    let nc = nats::connect("localhost").unwrap();
    let sc = stan::from_nats(nc)
        .upgrade_to_streaming("test-cluster", "clientId")
        .unwrap();

    let sub = sc.subscribe("darkslack").expect("could not subscribe");

    while let Some(msg) = sub.next_and_ack().unwrap() {
        eprintln!("{:?}", msg);
        let text = std::str::from_utf8(&msg.data).unwrap();
        eprintln!("{}", text);
        if text.contains("quit") {
            eprintln!("breaking");
            break;
        }
    }

    sub.unsubscribe().expect("fail to unsub");

    std::thread::sleep(std::time::Duration::from_secs(20));
}
