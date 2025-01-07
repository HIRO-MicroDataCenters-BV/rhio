use anyhow::Result;
use rhio::context_builder::ContextBuilder;
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
    info!("");
}
