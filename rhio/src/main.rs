use anyhow::Result;
use clap::{crate_description, crate_version};
use rhio::{built_info, context_builder::ContextBuilder};
use tracing::info;

fn main() -> Result<()> {
    let context_builder = ContextBuilder::from_cli()?;
    let context = context_builder.try_build_and_start()?;

    context.configure()?;

    log_hello_rhio();
    context.log_configuration();

    context.wait_for_termination()?;

    info!("shutting down");

    context.shutdown()
}

fn log_hello_rhio() {
    r#"      ___           ___                       ___
     /\  \         /\__\          ___        /\  \
    /::\  \       /:/  /         /\  \      /::\  \
   /:/\:\  \     /:/__/          \:\  \    /:/\:\  \
  /::\~\:\  \   /::\  \ ___      /::\__\  /:/  \:\  \
 /:/\:\ \:\__\ /:/\:\  /\__\  __/:/\/__/ /:/__/ \:\__\
 \/_|::\/:/  / \/__\:\/:/  / /\/:/  /    \:\  \ /:/  /
    |:|::/  /       \::/  /  \::/__/      \:\  /:/  /
    |:|\/__/        /:/  /    \:\__\       \:\/:/  /
    |:|  |         /:/  /      \/__/        \::/  /
     \|__|         \/__/                     \/__/ 
    "#
    .split("\n")
    .for_each(|line| info!("{}", line));
    print_startup_string(
        crate_description!(),
        crate_version!(),
        built_info::GIT_VERSION,
        built_info::TARGET,
        built_info::BUILT_TIME_UTC,
        built_info::RUSTC_VERSION,
    );
    info!("");
}

pub fn print_startup_string(
    pkg_description: &str,
    pkg_version: &str,
    git_version: Option<&str>,
    target: &str,
    built_time: &str,
    rustc_version: &str,
) {
    let git = match git_version {
        None => "".to_string(),
        Some(git) => format!(" (Git information: {git})"),
    };
    info!("Starting {pkg_description}");
    info!(
        "This is version {pkg_version}{git}, built for {target} by {rustc_version} at {built_time}",
    )
}
