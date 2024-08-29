use std::str::FromStr;

use tracing::Level;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn setup_tracing(filter: Option<String>) {
    let default = "rhio=INFO"
        .parse()
        .expect("hard-coded default directive should be valid");

    let builder = EnvFilter::builder().with_default_directive(default);

    let filter = if let Some(filter) = filter {
        let filter = match Level::from_str(&filter) {
            Ok(level) => format!("rhio={level}"),
            Err(_) => filter,
        };
        builder.parse_lossy(filter)
    } else {
        builder.parse_lossy("")
    };

    tracing_subscriber::registry()
        .with(Layer::default())
        .with(filter)
        .try_init()
        .ok();
}
