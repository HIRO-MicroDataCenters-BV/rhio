pub mod api;
pub mod configuration;
pub mod operations;
pub mod rhio_controller;
pub mod service_resource;
pub mod stream_controller;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
