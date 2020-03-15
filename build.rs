fn main() {
    prost_build::compile_protos(&["src/streaming/protocol.proto"],
                                &["src/streaming/"]).unwrap();
}
