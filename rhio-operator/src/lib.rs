#![allow(clippy::result_large_err)]

pub mod api;
pub mod cli;
pub mod configuration;
pub mod operations;
pub mod rhio;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
