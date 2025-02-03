use anyhow::Result;
use axum::async_trait;
use axum_prometheus::{
    metrics::set_global_recorder,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
};
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
    recorder_handle: PrometheusHandle,
}

impl RhioApiImpl {
    pub fn new(config: Config) -> Result<RhioApiImpl> {
        let recorder_handle = setup_metrics_recorder()?;
        Ok(RhioApiImpl {
            config,
            recorder_handle,
        })
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
        if let Some(publish) = &self.config.publish {
            for subject in &publish.nats_subjects {
                let status = MessageStreamPublishStatus {
                    status: ObjectStatus::Activated,
                    stream: subject.stream_name.to_owned(),
                    subject: subject.subject.to_string(),
                };
                streams.published.push(status);
            }
            for bucket in &publish.s3_buckets {
                let status = ObjectStorePublishStatus {
                    status: ObjectStatus::Activated,
                    bucket: bucket.to_owned(),
                };
                stores.published.push(status);
            }
        }
        if let Some(subscribe) = &self.config.subscribe {
            for subject in &subscribe.nats_subjects {
                let status = MessageStreamSubscribeStatus {
                    status: ObjectStatus::Activated,
                    stream: subject.stream_name.to_owned(),
                    subject: subject.subject.to_string(),
                    source: subject.public_key.to_hex(),
                };
                streams.subscribed.push(status);
            }
            for bucket in &subscribe.s3_buckets {
                let status = ObjectStoreSubscribeStatus {
                    status: ObjectStatus::Activated,
                    remote_bucket: bucket.remote_bucket_name.to_owned(),
                    local_bucket: bucket.local_bucket_name.to_owned(),
                    source: bucket.public_key.to_hex(),
                };
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
