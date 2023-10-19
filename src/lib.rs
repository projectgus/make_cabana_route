pub mod input;
pub mod qlog;
pub mod video;

// Type for nanosecond timestamps
// TODO: Try using Duration for this and converting to nanos at time of encoding
pub type Nanos = i64;

// These files are part of comma.ai Cereal project and are
// Copyright (c) 2020, Comma.ai, Inc., distributed under MIT License
pub mod car_capnp {
    include!(concat!(env!("OUT_DIR"), "/car_capnp.rs"));
}
pub mod legacy_capnp {
    include!(concat!(env!("OUT_DIR"), "/legacy_capnp.rs"));
}
pub mod log_capnp {
    include!(concat!(env!("OUT_DIR"), "/log_capnp.rs"));
}
