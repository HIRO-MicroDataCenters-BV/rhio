use axum::Json;
use rhio_config::configuration::Config;
use rhio_http_api::status::{
    HealthStatus, MessageStreamPublishStatus, MessageStreamSubscribeStatus, MessageStreams,
    ObjectStatus, ObjectStorePublishStatus, ObjectStoreSubscribeStatus, ObjectStores,
};

pub async fn status_handler(config: &Config) -> Json<HealthStatus> {
    let status = "running".to_string();
    let mut streams = MessageStreams {
        published: vec![],
        subscribed: vec![],
    };
    let mut stores = ObjectStores {
        published: vec![],
        subscribed: vec![],
    };
    if let Some(publish) = &config.publish {
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
    if let Some(subscribe) = &config.subscribe {
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

    Json(HealthStatus {
        streams,
        stores,
        status,
    })
}
