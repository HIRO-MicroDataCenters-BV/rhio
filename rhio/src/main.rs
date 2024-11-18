use anyhow::{Context, Result};
use rhio::config::{load_config, LocalNatsSubject, RemoteNatsSubject, RemoteS3Bucket};
use rhio::tracing::setup_tracing;
use rhio::{Node, Publication, Subscription};
use rhio_core::load_private_key_from_file;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    setup_tracing(config.log_level.clone());

    let private_key =
        load_private_key_from_file(&config.node.private_key_path).context(format!(
            "could not load private key from file {}",
            config.node.private_key_path.display(),
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
            node.publish(Publication::Bucket {
                bucket_name,
                public_key,
            })
            .await?;
        }

        for LocalNatsSubject {
            stream_name,
            subject,
        } in publish.nats_subjects
        {
            // Assign our own public key to NATS subject info.
            node.publish(Publication::Subject {
                stream_name,
                subject,
                public_key,
            })
            .await?;
        }
    };

    if let Some(subscribe) = config.subscribe {
        for RemoteS3Bucket {
            bucket_name,
            public_key,
        } in subscribe.s3_buckets
        {
            node.subscribe(Subscription::Bucket {
                bucket_name,
                public_key,
            })
            .await?;
        }

        for RemoteNatsSubject {
            stream_name,
            subject,
            public_key,
        } in subscribe.nats_subjects
        {
            node.subscribe(Subscription::Subject {
                stream_name,
                subject,
                public_key,
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
