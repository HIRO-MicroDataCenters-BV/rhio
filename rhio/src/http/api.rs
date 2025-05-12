use anyhow::Result;
use async_trait::async_trait;
use axum_prometheus::{
    metrics::set_global_recorder,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
};
use rhio_blobs::{BucketState, BucketStatus, S3Store};
use rhio_config::configuration::Config;
use rhio_http_api::{
    api::RhioApi,
    status::{
        HealthStatus, MessageStreamPublishStatus, MessageStreamSubscribeStatus, MessageStreams,
        ObjectStatus, ObjectStorePublishStatus, ObjectStoreSubscribeStatus, ObjectStores,
    },
};
use tracing::warn;

/// `RhioApiImpl` is an implementation of the `RhioApi` trait, providing methods
/// to check the health status and retrieve metrics of the Rhio service.
///
/// # Fields
/// - `config`: Configuration settings for the Rhio service.
/// - `recorder_handle`: Handle to the Prometheus metrics recorder.
///
/// # Methods
/// - `new(config: Config) -> Result<RhioApiImpl>`: Creates a new instance of `RhioApiImpl`
///   with the provided configuration and sets up the metrics recorder.
///
/// # Trait Implementations
/// - `RhioApi`: Implements the `health` and `metrics` methods to provide health status
///   and metrics information respectively.
///
pub struct RhioApiImpl {
    config: Config,
    store: S3Store,
    recorder_handle: PrometheusHandle,
}

impl RhioApiImpl {
    pub fn new(config: Config, store: S3Store) -> Result<RhioApiImpl> {
        let recorder_handle = setup_metrics_recorder()?;
        Ok(RhioApiImpl {
            config,
            recorder_handle,
            store,
        })
    }

    fn to_object_state(state: &BucketState) -> ObjectStatus {
        match state {
            BucketState::Active => ObjectStatus::Active,
            BucketState::NotInitialized => ObjectStatus::New,
            BucketState::Inactive => ObjectStatus::Inactive,
        }
    }

    fn to_bucket_publish_status(
        bucket: &String,
        status: &BucketStatus,
    ) -> ObjectStorePublishStatus {
        ObjectStorePublishStatus {
            status: RhioApiImpl::to_object_state(&status.state),
            bucket: bucket.to_owned(),
            last_error: status.last_error.clone(),
            last_check_time: status.last_check_time,
        }
    }

    fn to_bucket_subscribe_status(
        source: &String,
        remote_bucket: &String,
        local_bucket: &String,
        status: &BucketStatus,
    ) -> ObjectStoreSubscribeStatus {
        ObjectStoreSubscribeStatus {
            status: RhioApiImpl::to_object_state(&status.state),
            remote_bucket: remote_bucket.to_owned(),
            local_bucket: local_bucket.to_owned(),
            source: source.to_owned(),
            last_error: status.last_error.clone(),
            last_check_time: status.last_check_time,
        }
    }
}

#[async_trait]
impl RhioApi for RhioApiImpl {
    async fn health(&self) -> Result<HealthStatus> {
        let mut streams = MessageStreams {
            published: vec![],
            subscribed: vec![],
        };
        let mut stores = ObjectStores {
            published: vec![],
            subscribed: vec![],
        };
        let statuses = self.store.statuses();
        if let Some(publish) = &self.config.publish {
            for subject in &publish.nats_subjects {
                let status = MessageStreamPublishStatus {
                    status: ObjectStatus::Active,
                    stream: subject.stream_name.to_owned(),
                    subject: subject.subject.to_string(),
                    last_error: None,
                    last_check_time: None,
                };
                streams.published.push(status);
            }
            for bucket in &publish.s3_buckets {
                let status = statuses
                    .get(bucket)
                    .map(|status| RhioApiImpl::to_bucket_publish_status(bucket, status))
                    .unwrap_or_else(|| ObjectStorePublishStatus::to_unknown(bucket));
                stores.published.push(status);
            }
        }

        if let Some(subscribe) = &self.config.subscribe {
            for subject in &subscribe.nats_subjects {
                let status = MessageStreamSubscribeStatus {
                    status: ObjectStatus::Active,
                    stream: subject.stream_name.to_owned(),
                    subject: subject.subject.to_string(),
                    source: subject.public_key.to_hex(),
                    last_error: None,
                    last_check_time: None,
                };
                streams.subscribed.push(status);
            }
            for bucket in &subscribe.s3_buckets {
                let status = statuses
                    .get(&bucket.local_bucket_name)
                    .map(|status| {
                        RhioApiImpl::to_bucket_subscribe_status(
                            &bucket.public_key.to_hex(),
                            &bucket.remote_bucket_name,
                            &bucket.local_bucket_name,
                            status,
                        )
                    })
                    .unwrap_or_else(|| {
                        ObjectStoreSubscribeStatus::to_unknown(
                            &bucket.public_key.to_hex(),
                            &bucket.remote_bucket_name,
                            &bucket.local_bucket_name,
                        )
                    });

                stores.subscribed.push(status);
            }
        }

        Ok(HealthStatus {
            streams,
            stores,
            status: rhio_http_api::status::ServiceStatus::Running,
            msg: None,
        })
    }

    async fn metrics(&self) -> Result<String> {
        Ok(self.recorder_handle.render())
    }
}

fn setup_metrics_recorder() -> Result<PrometheusHandle> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let maybe_success = set_global_recorder(recorder);
    if let Err(e) = &maybe_success {
        let msg = format!("{}", e);
        if msg.contains(
            "attempted to set a recorder after the metrics system was already initialized",
        ) {
            warn!("global recorder is possibly reused.")
        } else {
            maybe_success?;
        }
    }
    Ok(handle)
}
