use anyhow::Result;
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use serde::{Deserialize, Serialize};

const DEFAULT_BIND_PORT: u16 = 4012;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub bind_port: u16,
}

#[derive(Parser, Serialize, Debug)]
#[command(
    name = "rohi",
    about = "HIRO Blob Syncing Node",
    long_about = None,
    version
)]
struct Cli {
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bind_port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_port: DEFAULT_BIND_PORT,
        }
    }
}

pub fn load_config() -> Result<Config> {
    let cli = Cli::parse();

    let figment = Figment::from(Serialized::defaults(Config::default()));

    let config = figment
        .merge(Env::raw())
        .merge(Serialized::defaults(cli))
        .extract()?;

    Ok(config)
}
