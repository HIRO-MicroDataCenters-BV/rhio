use std::collections::HashMap;

use anyhow::{Context, Result};
use figment::providers::Env;
use p2panda_core::{PrivateKey, PublicKey};
use rhio::config::{load_config, LocalNatsSubject, RemoteNatsSubject, RemoteS3Bucket, PRIVATE_KEY_ENV};
use rhio::tracing::setup_tracing;
use rhio::{
    http_server, FilesSubscription, FilteredMessageStream, MessagesSubscription, Node, Publication,
    StreamName, Subscription,
};
use rhio_core::{load_private_key_from_file, Subject};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    setup_tracing(config.log_level.clone());

    // Load the private key from either an environment variable _or_ a file specified in the
    // config. The environment variable takes priority.
    let private_key = match Env::var(PRIVATE_KEY_ENV) {
        Some(private_key_hex) => PrivateKey::try_from(&hex::decode(&private_key_hex)?[..])?,
        None => load_private_key_from_file(&config.node.private_key_path).context(format!(
            "could not load private key from file {}",
            config.node.private_key_path.display(),
        ))?,
    };

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
    info!("‣ health endpoint:");
    info!("  - localhost:{}", config.node.http_bind_port);

    if let Some(publish) = config.publish {
        for bucket_name in publish.s3_buckets {
            // Assign our own public key to S3 bucket info.
            node.publish(Publication::Files {
                bucket_name,
                public_key,
            })
            .await?;
        }

        // Multiple subjects can be used on top of a stream and we want to group them over one
        // public key and stream name pair. This leaves us with the following structure:
        //
        // Streams      Public Key       Subjects        Topic Id (for Gossip)
        // =======      ==========       ========        =====================
        // 1            I                A               a
        // 2            I                B               a
        // 3            II               A               b
        // 1            II               B               b
        // 1            II               C               b
        let mut stream_public_key_map = HashMap::<StreamName, Vec<Subject>>::new();
        for LocalNatsSubject {
            stream_name,
            subject,
        } in publish.nats_subjects
        {
            stream_public_key_map
                .entry(stream_name)
                .and_modify(|subjects| {
                    subjects.push(subject.clone());
                })
                .or_insert(vec![subject]);
        }

        for (stream_name, subjects) in stream_public_key_map.into_iter() {
            node.publish(Publication::Messages {
                filtered_stream: FilteredMessageStream {
                    subjects,
                    stream_name,
                },
                // Assign our own public key to NATS subject info.
                public_key,
            })
            .await?;
        }
    };

    if let Some(subscribe) = config.subscribe {
        for RemoteS3Bucket {
            bucket_name,
            public_key: remote_public_key,
        } in subscribe.s3_buckets
        {
            node.subscribe(Subscription::Files(FilesSubscription {
                bucket_name,
                public_key: remote_public_key,
            }))
            .await?;
        }

        // Multiple subjects can be used on top of a stream and we want to group them over one
        // public key and stream name pair.
        let mut stream_public_key_map = HashMap::<(StreamName, PublicKey), Vec<Subject>>::new();
        for RemoteNatsSubject {
            stream_name,
            subject,
            public_key: remote_public_key,
        } in subscribe.nats_subjects
        {
            stream_public_key_map
                .entry((stream_name, remote_public_key))
                .and_modify(|subjects| {
                    subjects.push(subject.clone());
                })
                .or_insert(vec![subject]);
        }

        // Finally we want to group these subscriptions by public key.
        let mut subscription_map = HashMap::<PublicKey, Vec<FilteredMessageStream>>::new();
        for ((stream_name, remote_public_key), subjects) in stream_public_key_map.into_iter() {
            let filtered_stream = FilteredMessageStream {
                subjects,
                stream_name,
            };
            subscription_map
                .entry(remote_public_key)
                .and_modify(|filtered_streams| filtered_streams.push(filtered_stream.clone()))
                .or_insert(vec![filtered_stream]);
        }

        for (remote_public_key, filtered_streams) in subscription_map.into_iter() {
            let subscription = Subscription::Messages(MessagesSubscription {
                filtered_streams,
                public_key: remote_public_key,
            });
            node.subscribe(subscription).await?;
        }
    };

    if let Err(err) = tokio::spawn(http_server::run(config.node.http_bind_port)).await? {
        error!("failed to start http server: {err}");
        return Ok(());
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
