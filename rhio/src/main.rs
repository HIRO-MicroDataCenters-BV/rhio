use anyhow::{Context, Result};
use rhio::config::{load_config, NatsSubject};
use rhio::tracing::setup_tracing;
use rhio::{Node, Publication, Subscription};
use rhio_core::{load_private_key_from_file, ScopedBucket, ScopedSubject};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    setup_tracing(config.log_level.clone());

    let private_key =
        load_private_key_from_file(config.node.private_key.clone()).context(format!(
            "could not load private key from file {}",
            config.node.private_key.display(),
        ))?;
    let public_key = private_key.public_key();

    let node = Node::spawn(config.clone(), private_key).await?;

    hello_rhio();
    let addresses: Vec<String> = node
        .direct_addresses()
        .iter()
        .map(|addr| addr.to_string())
        .collect();
    info!("‣ network id:");
    info!("  - {}", config.node.network_id);
    info!("‣ node public key:");
    info!("  - {}", public_key);
    info!("‣ node addresses:");
    for address in addresses {
        info!("  - {}", address);
    }

    if let Some(publish) = config.publish {
        for bucket_name in publish.s3_buckets {
            // Assign our own public key to S3 bucket info.
            let bucket = ScopedBucket::new(&bucket_name, public_key);
            node.publish(Publication::Bucket { bucket }).await?;
        }

        for NatsSubject {
            stream_name,
            subject,
        } in publish.nats_subjects
        {
            if subject.public_key() != public_key {
                warn!(
                    "given public key in 'publish' configuration does not match actual local
                    public key"
                );
            }

            // Assign our own public key to NATS subject info.
            let subject = ScopedSubject::new(public_key, &subject.subject().to_string());
            node.publish(Publication::Subject {
                stream_name,
                subject,
            })
            .await?;
        }
    };

    if let Some(subscribe) = config.subscribe {
        for bucket in subscribe.s3_buckets {
            node.subscribe(Subscription::Bucket { bucket }).await?;
        }

        for NatsSubject {
            stream_name,
            subject,
        } in subscribe.nats_subjects
        {
            node.subscribe(Subscription::Subject {
                stream_name,
                subject,
            })
            .await?;
        }
    };

    tokio::signal::ctrl_c().await?;

    info!("");
    info!("shutting down");
    node.shutdown().await?;

    Ok(())
}

fn hello_rhio() {
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
}
