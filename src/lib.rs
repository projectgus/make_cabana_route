pub mod qlog;
pub mod video;

extern crate ffmpeg;

// Type for nanosecond timestamps
// TODO: Try using Duration for this and converting to nanos at time of encoding
pub type Nanos = i64;

pub mod car_capnp {
    include!(concat!(env!("OUT_DIR"), "/car_capnp.rs"));
}
pub mod legacy_capnp {
    include!(concat!(env!("OUT_DIR"), "/legacy_capnp.rs"));
}
pub mod log_capnp {
    include!(concat!(env!("OUT_DIR"), "/log_capnp.rs"));
}
