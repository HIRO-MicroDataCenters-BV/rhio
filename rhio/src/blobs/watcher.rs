use std::time::Duration;

use rhio_blobs::S3Store;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub enum S3WatcherEvent {}

const POLL_FREQUENCY: Duration = Duration::from_secs(1);

/// Service watching the S3 buckets and p2p blob interface to inform us on newly detected objects.
#[derive(Debug)]
pub struct S3Watcher {
    event_tx: broadcast::Sender<S3WatcherEvent>,
}

impl S3Watcher {
    pub fn new(store: S3Store) -> Self {
        let (event_tx, _) = broadcast::channel(64);
        let watcher = Self { event_tx };

        tokio::spawn(async move {
            loop {
                for bucket in store.buckets() {
                    println!("CHECK LIST for {}", bucket.name());

                    let list_results = bucket.list("/".to_owned(), None).await.unwrap();
                    for list in list_results {
                        for object in list.contents {
                            println!("{:?}", object);
                        }
                    }
                }

                tokio::time::sleep(POLL_FREQUENCY).await;
            }
        });

        watcher
    }

    pub fn subscribe(&mut self) -> broadcast::Receiver<S3WatcherEvent> {
        self.event_tx.subscribe()
    }
}
