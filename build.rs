fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("cereal")
        .file("cereal/log.capnp")
        .file("cereal/car.capnp")
        .file("cereal/legacy.capnp")
        .run()
        .expect("schema compiler command");
}
