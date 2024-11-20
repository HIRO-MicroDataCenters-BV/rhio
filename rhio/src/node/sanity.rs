use anyhow::{bail, Result};

use crate::{Publication, Subscription};

pub fn validate_publication_config(
    existing_publications: &Vec<Publication>,
    new_publication: &Publication,
) -> Result<()> {
    // 1. Published bucket names need to be unique.
    // 2. Published NATS subject need to be unique.
    for existing_publication in existing_publications {
        match &new_publication {
            Publication::Files { bucket_name, .. } => match existing_publication {
                Publication::Files {
                    bucket_name: existing_bucket_name,
                    ..
                } => {
                    if existing_bucket_name == bucket_name {
                        bail!(
                            "publish config contains duplicate S3 bucket {}",
                            bucket_name
                        );
                    }
                }
                Publication::Messages { .. } => {
                    continue;
                }
            },
            Publication::Messages {
                filtered_stream, ..
            } => match existing_publication {
                Publication::Files { .. } => continue,
                Publication::Messages {
                    filtered_stream: existing_filtered_stream,
                    ..
                } => {
                    for new_subject in &filtered_stream.0 {
                        if existing_filtered_stream.0.contains(new_subject) {
                            bail!(
                                "publish config contains duplicate NATS subject '{}'",
                                new_subject
                            );
                        }
                    }
                }
            },
        }
    }

    Ok(())
}

pub fn validate_subscription_config(
    existing_publications: &Vec<Publication>,
    existing_subscriptions: &Vec<Subscription>,
    new_subscription: &Subscription,
) -> Result<()> {
    // 1. Subscribed bucket names can't be used for publishing as well.
    for existing_publication in existing_publications {
        match &new_subscription {
            Subscription::Files { bucket_name, .. } => match existing_publication {
                Publication::Files {
                    bucket_name: existing_bucket_name,
                    ..
                } => {
                    if existing_bucket_name == bucket_name {
                        bail!(
                            "bucket '{}' for subscribe config is already used in publish config",
                            bucket_name
                        );
                    }
                }
                Publication::Messages { .. } => {
                    continue;
                }
            },
            Subscription::Messages { .. } => {
                continue;
            }
        }
    }

    // 2. Subscribed public key can't be re-used for another bucket.
    // 3. Subscribed NATS subject + public key tuples need to be unique.
    for existing_subscribtion in existing_subscriptions {
        match &new_subscription {
            Subscription::Files { public_key, .. } => match existing_subscribtion {
                Subscription::Files {
                    public_key: existing_public_key,
                    ..
                } => {
                    if existing_public_key == public_key {
                        bail!(
                            "public key {} is used multiple times in subscribe S3 config",
                            public_key
                        );
                    }
                }
                Subscription::Messages { .. } => {
                    continue;
                }
            },
            Subscription::Messages {
                public_key,
                filtered_stream,
                ..
            } => match existing_subscribtion {
                Subscription::Files { .. } => continue,
                Subscription::Messages {
                    filtered_stream: existing_filtered_stream,
                    public_key: existing_public_key,
                    ..
                } => {
                    if public_key != existing_public_key {
                        continue;
                    }

                    for new_subject in &filtered_stream.0 {
                        if existing_filtered_stream.0.contains(new_subject) {
                            bail!(
                                "public key {} and subject '{}' is used multiple times in subscribe NATS config",
                                public_key,
                                new_subject,
                            );
                        }
                    }
                }
            },
        }
    }

    Ok(())
}
