use rhio_config::configuration::NatsCredentials;
use s3::creds::Credentials;
use stackable_operator::cli::ProductOperatorRun;

use crate::{configuration::secret::Secret, rhio::private_key::PrivateKey};

#[derive(clap::Parser)]
pub enum RhioCommand {
    CreatePrivateKeySecret(PrivateKeySecretArgs),
    CreateNatsSecret(NatsSecretArgs),
    CreateS3Secret(S3SecretArgs),
    #[clap(flatten)]
    Framework(stackable_operator::cli::Command<RhioRun>),
}

#[derive(clap::Parser, Debug, PartialEq, Eq)]
#[command(long_about = "")]
pub struct PrivateKeySecretArgs {}

impl PrivateKeySecretArgs {
    pub fn generate_secret(&self) -> anyhow::Result<()> {
        let private_key = p2panda_core::PrivateKey::new();
        let private_key = PrivateKey {
            secret_key: private_key.to_hex(),
            public_key: private_key.public_key().to_hex(),
        };
        Secret::<PrivateKey>::new("private_key_secret".into(), "default".into(), private_key)
            .print_yaml()?;
        Ok(())
    }
}

#[derive(clap::Parser, Debug, PartialEq, Eq)]
#[command(long_about = "")]
pub struct NatsSecretArgs {
    #[arg(long, short = 'u')]
    pub username: String,

    #[arg(long, short = 'p')]
    pub password: String,
}

impl NatsSecretArgs {
    pub fn generate_secret(&self) -> anyhow::Result<()> {
        let credentials = NatsCredentials {
            nkey: None,
            username: Some(self.username.to_owned()),
            password: Some(self.password.to_owned()),
            token: None,
        };
        Secret::<NatsCredentials>::new("nats_credentials".into(), "default".into(), credentials)
            .print_yaml()?;
        Ok(())
    }
}

#[derive(clap::Parser, Debug, PartialEq, Eq)]
#[command(long_about = "")]
pub struct S3SecretArgs {
    #[arg(long, short = 'a')]
    pub access_key: String,

    #[arg(long, short = 's')]
    pub secret_key: String,
}

impl S3SecretArgs {
    pub fn generate_secret(&self) -> anyhow::Result<()> {
        let credentials = Credentials {
            access_key: Some(self.access_key.to_owned()),
            secret_key: Some(self.secret_key.to_owned()),
            security_token: None,
            session_token: None,
            expiration: None,
        };
        Secret::<Credentials>::new("s3_credentials".into(), "default".into(), credentials)
            .print_yaml()?;
        Ok(())
    }
}

#[derive(clap::Parser)]
#[clap(about, author)]
pub struct Opts {
    #[clap(subcommand)]
    pub cmd: RhioCommand,
}

#[derive(clap::Parser)]
pub struct RhioRun {
    #[clap(flatten)]
    pub common: ProductOperatorRun,
}
