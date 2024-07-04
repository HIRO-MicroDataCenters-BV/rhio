use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn setup_tracing() {
    tracing_subscriber::registry()
        .with(Layer::default())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}
