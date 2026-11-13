fn main() {
    connectrpc_build::Config::new()
        .files(&["../proto/sample/v1/sample.proto"])
        .includes(&["../proto/"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}
