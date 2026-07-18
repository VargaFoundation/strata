fn main() {
    // Use bundled protoc from protobuf-src (no system protoc required).
    std::env::set_var("PROTOC", protobuf_src::protoc());

    tonic_build::compile_protos("proto/raft.proto")
        .expect("failed to compile Raft protobuf definitions");
}
