use std::str::FromStr;

use tracing::Level;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Setup logging with the help of the `tracing` crate.
///
/// The verbosity and targets can be configured with a filter string:
///
/// 1. When no filter is set the default "rhio=INFO" filter will be applied
/// 2. When only a level was given ("INFO", "DEBUG", etc.) it will be used for the rhio target
///    "rhio={level}"
/// 3. When the string begins with an "=", then the log level will be applied for _all_ targets.
///    This is equivalent to only setting the level "{level}"
/// 4. When the string specifies a target and a level it will be used as-is: "{filter}", for
///    example "tokio=TRACE"
pub fn setup_tracing(filter: Option<String>) {
    let default = "rhio=INFO"
        .parse()
        .expect("hard-coded default directive should be valid");

    let builder = EnvFilter::builder().with_default_directive(default);

    let filter = if let Some(filter) = filter {
        if let Some(all_targets_level) = filter.strip_prefix("=") {
            all_targets_level.to_string()
        } else {
            match Level::from_str(&filter) {
                Ok(level) => format!("rhio={level}"),
                Err(_) => filter,
            }
        }
    } else {
        String::default()
    };
    let filter = builder.parse_lossy(filter);

    tracing_subscriber::registry()
        .with(Layer::default())
        .with(filter)
        .try_init()
        .ok();
}
