use crate::rhio::builders::STACKABLE_VENDOR_VALUE_HIRO;
use crate::rhio::controller::Result;
use crate::{
    api::{
        message_stream::ReplicatedMessageStream,
        message_stream_subscription::ReplicatedMessageStreamSubscription,
        object_store::ReplicatedObjectStore,
        object_store_subscription::ReplicatedObjectStoreSubscription,
        service::{RhioConfig, RhioService},
    },
    rhio::controller::{
        BuildConfigMapSnafu, GetReplicatedMessageStreamsSnafu, InvalidNatsSubjectSnafu,
        MetadataBuildSnafu, ObjectHasNoNamespaceSnafu, ObjectMissingMetadataForOwnerRefSnafu,
        RhioConfigurationSerializationSnafu,
    },
};
use p2panda_core::PublicKey;
use rhio_config::configuration::{
    Config, KnownNode, LocalNatsSubject, NatsConfig, NatsCredentials, NodeConfig, PublishConfig,
    RemoteNatsSubject, RemoteS3Bucket, S3Config, SubscribeConfig,
};
use s3::creds::Credentials;
use snafu::{OptionExt, ResultExt};
use stackable_operator::client::Client;
use stackable_operator::k8s_openapi::api::core::v1::ConfigMap;
use stackable_operator::kube::{api::ListParams, runtime::reflector::ObjectRef, Resource};
use stackable_operator::kvp::ObjectLabels;
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    kvp::consts::STACKABLE_VENDOR_KEY,
};
use std::hash::{Hash, Hasher};
use std::{collections::BTreeMap, hash::DefaultHasher, str::FromStr};

const RHIO_PRIVATE_KEY_PATH: &str = "/etc/rhio/private-key.txt";
pub const RHIO_BIND_PORT_DEFAULT: u16 = 9102;
pub const RHIO_BIND_HTTP_PORT_DEFAULT: u16 = 8080;
pub const RHIO_CONFIG_MAP_ENTRY: &str = "config.yaml";

pub struct RhioConfigurationResources {
    rhio: RhioService,
    ros: Vec<ReplicatedMessageStream>,
    ross: Vec<ReplicatedMessageStreamSubscription>,
    rms: Vec<ReplicatedObjectStore>,
    rmss: Vec<ReplicatedObjectStoreSubscription>,
}

impl RhioConfigurationResources {
    pub fn from(
        rhio_service: &RhioService,
        ros: Vec<ReplicatedMessageStream>,
        ross: Vec<ReplicatedMessageStreamSubscription>,
        rms: Vec<ReplicatedObjectStore>,
        rmss: Vec<ReplicatedObjectStoreSubscription>,
    ) -> RhioConfigurationResources {
        RhioConfigurationResources {
            ros,
            ross,
            rms,
            rmss,
            rhio: rhio_service.clone(),
        }
    }

    pub async fn load(
        client: Client,
        rhio_service: &RhioService,
    ) -> Result<RhioConfigurationResources> {
        let streams = client
            .list::<ReplicatedMessageStream>(
                rhio_service
                    .meta()
                    .namespace
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
                &ListParams::default(),
            )
            .await
            .context(GetReplicatedMessageStreamsSnafu)?
            .into_iter()
            .filter(|stream| stream.metadata.namespace == rhio_service.metadata.namespace)
            .collect::<Vec<ReplicatedMessageStream>>();

        let stores = client
            .list::<ReplicatedObjectStore>(
                rhio_service
                    .meta()
                    .namespace
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
                &ListParams::default(),
            )
            .await
            .context(GetReplicatedMessageStreamsSnafu)?
            .into_iter()
            .filter(|stream| stream.metadata.namespace == rhio_service.metadata.namespace)
            .collect::<Vec<ReplicatedObjectStore>>();

        let stream_subscriptions = client
            .list::<ReplicatedMessageStreamSubscription>(
                rhio_service
                    .meta()
                    .namespace
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
                &ListParams::default(),
            )
            .await
            .context(GetReplicatedMessageStreamsSnafu)?
            .into_iter()
            .filter(|sub| sub.metadata.namespace == rhio_service.metadata.namespace)
            .collect::<Vec<ReplicatedMessageStreamSubscription>>();

        let stores_subscriptions = client
            .list::<ReplicatedObjectStoreSubscription>(
                rhio_service
                    .meta()
                    .namespace
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
                &ListParams::default(),
            )
            .await
            .context(GetReplicatedMessageStreamsSnafu)?
            .into_iter()
            .filter(|sub| sub.metadata.namespace == rhio_service.metadata.namespace)
            .collect::<Vec<ReplicatedObjectStoreSubscription>>();

        Ok(RhioConfigurationResources {
            ros: streams,
            ross: stream_subscriptions,
            rms: stores,
            rmss: stores_subscriptions,
            rhio: rhio_service.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_rhio_configmap(
        &self,
        labels: ObjectLabels<RhioService>,
    ) -> Result<(ConfigMap, String)> {
        let published_nats_subjects = self
            .ros
            .iter()
            .flat_map(|stream| {
                stream
                    .spec
                    .subjects
                    .iter()
                    .map(|published_subject| {
                        rhio_core::Subject::from_str(published_subject)
                            .context(InvalidNatsSubjectSnafu {
                                subject: published_subject.to_owned(),
                            })
                            .map(|subject| LocalNatsSubject {
                                stream_name: stream.spec.stream_name.to_owned(),
                                subject,
                            })
                    })
                    .collect::<Vec<Result<LocalNatsSubject>>>()
            })
            .collect::<Result<Vec<LocalNatsSubject>>>()?;

        let subscribed_subjects = self
            .ross
            .iter()
            .flat_map(|sub| {
                let public_key = PublicKey::from_str(&sub.spec.public_key).unwrap();
                sub.spec
                    .subscriptions
                    .iter()
                    .map(|spec| {
                        let subject = rhio_core::Subject::from_str(&spec.subject).context(
                            InvalidNatsSubjectSnafu {
                                subject: spec.subject.to_owned(),
                            },
                        )?;

                        Ok(RemoteNatsSubject {
                            stream_name: spec.stream.to_owned(),
                            public_key,
                            subject,
                        })
                    })
                    .collect::<Vec<Result<RemoteNatsSubject>>>()
            })
            .collect::<Result<Vec<RemoteNatsSubject>>>()?;

        let published_buckets = self
            .rms
            .iter()
            .flat_map(|store| store.spec.buckets.iter())
            .cloned()
            .collect::<Vec<String>>();

        let subscribed_buckets = self
            .rmss
            .iter()
            .flat_map(|sub| {
                let public_key = PublicKey::from_str(&sub.spec.public_key).unwrap(); // TODO unwrap
                sub.spec
                    .buckets
                    .iter()
                    .map(|bucket| RemoteS3Bucket {
                        remote_bucket_name: bucket.remote_bucket.to_owned(),
                        local_bucket_name: bucket.local_bucket.to_owned(),
                        public_key,
                    })
                    .collect::<Vec<RemoteS3Bucket>>()
            })
            .collect::<Vec<RemoteS3Bucket>>();

        let config = self.build_rhio_configuration(
            &self.rhio.spec.configuration,
            published_nats_subjects,
            subscribed_subjects,
            published_buckets,
            subscribed_buckets,
        )?;
        let rhio_configuration =
            serde_json::to_string(&config).context(RhioConfigurationSerializationSnafu)?;

        let mut metadata = ObjectMetaBuilder::new()
            .name_and_namespace(&self.rhio)
            .ownerreference_from_resource(&self.rhio, None, Some(true))
            .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                rhio_service: ObjectRef::from_obj(&self.rhio),
            })?
            .with_recommended_labels(labels)
            .context(MetadataBuildSnafu)?
            .build();

        metadata.labels.get_or_insert(BTreeMap::new()).insert(
            STACKABLE_VENDOR_KEY.into(),
            STACKABLE_VENDOR_VALUE_HIRO.into(),
        );
        let mut hasher = DefaultHasher::new();
        rhio_configuration.hash(&mut hasher);
        let config_hash = hasher.finish().to_string();

        let config_map = ConfigMapBuilder::new()
            .metadata(metadata)
            .add_data(RHIO_CONFIG_MAP_ENTRY, rhio_configuration)
            .build()
            .context(BuildConfigMapSnafu)?;

        Ok((config_map, config_hash))
    }

    fn build_rhio_configuration(
        &self,
        spec_config: &RhioConfig,
        nats_subjects: Vec<LocalNatsSubject>,
        subscribe_subjects: Vec<RemoteNatsSubject>,
        s3_buckets: Vec<String>,
        subscribe_buckets: Vec<RemoteS3Bucket>,
    ) -> Result<Config> {
        let known_nodes = spec_config
            .nodes
            .iter()
            .map(|n| KnownNode {
                public_key: PublicKey::from_str(&n.public_key).unwrap(),
                direct_addresses: n.endpoints.to_owned(),
            })
            .collect();
        let credentials = spec_config
            .nats
            .clone()
            .credentials
            .map(|c| NatsCredentials {
                nkey: None,
                username: Some(c.username.to_owned()),
                password: Some(c.password.to_owned()),
                token: None,
            });

        let nats = NatsConfig {
            endpoint: spec_config.nats.endpoint.to_owned(),
            credentials,
        };
        let s3 = spec_config.s3.as_ref().map(|s3_conf| {
            let credentials = s3_conf.credentials.as_ref().map(|cred| Credentials {
                access_key: Some(cred.access_key.to_owned()),
                secret_key: Some(cred.secret_key.to_owned()),
                security_token: None,
                session_token: None,
                expiration: None,
            });
            S3Config {
                endpoint: s3_conf.endpoint.to_owned(),
                region: s3_conf.region.to_owned(),
                credentials,
            }
        });
        let config = Config {
            node: NodeConfig {
                bind_port: RHIO_BIND_PORT_DEFAULT,
                http_bind_port: RHIO_BIND_HTTP_PORT_DEFAULT,
                known_nodes,
                private_key_path: RHIO_PRIVATE_KEY_PATH.into(),
                network_id: spec_config.network_id.to_owned(),
                protocol: None,
            },
            s3,
            nats,
            log_level: None,
            publish: Some(PublishConfig {
                s3_buckets,
                nats_subjects,
            }),
            subscribe: Some(SubscribeConfig {
                s3_buckets: subscribe_buckets,
                nats_subjects: subscribe_subjects,
            }),
        };
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rhio_config::configuration::{
        Config, KnownNode, NatsConfig, NatsCredentials, NodeConfig, PublishConfig, SubscribeConfig,
    };

    use super::RhioConfigurationResources;
    use crate::api::message_stream::ReplicatedMessageStream;
    use crate::api::message_stream_subscription::ReplicatedMessageStreamSubscription;
    use crate::api::object_store::ReplicatedObjectStore;
    use crate::api::object_store_subscription::ReplicatedObjectStoreSubscription;
    use crate::rhio::controller::Result;
    use crate::{api::service::RhioService, rhio::builders::build_recommended_labels};
    use snafu::{OptionExt, ResultExt};

    #[test]
    fn test_minimal_config() -> Result<()> {
        let rhio_yaml = r#"
        apiVersion: rhio.hiro.io/v1
        kind: RhioService
        metadata:
            name: test-service
            uid: 1
        spec:
            clusterConfig:
                listenerClass: disabled
                gracefulShutdownTimeout: 1s

            image:
                custom: ghcr.io/hiro-microdatacenters-bv/rhio-dev:1.0.1
                productVersion: 1.0.1
                pullPolicy: IfNotPresent      

            configuration:
                networkId: test
                privateKeySecret: secret-key
                nodes: []
                s3: null
                nats:
                    endpoint: nats://nats-jetstream.dkg-engine.svc.cluster.local:4222
                    credentials: null
        "#;
        let rhio: RhioService = serde_yaml::from_str(rhio_yaml).expect("illegal test input");

        let config_resources =
            RhioConfigurationResources::from(&rhio, vec![], vec![], vec![], vec![]);

        let labels = build_recommended_labels(&rhio, "controller", "1.0.1", "role", "role_group");
        let (cm, hash) = config_resources
            .build_rhio_configmap(labels)
            .expect("unable to build rhio config");

        let config = cm.data.unwrap().get("config.yaml").unwrap().to_owned();

        let expected_config = Config {
            s3: None,
            nats: NatsConfig {
                endpoint: "nats://nats-jetstream.dkg-engine.svc.cluster.local:4222".into(),
                credentials: None,
            },
            node: NodeConfig {
                bind_port: 9102,
                http_bind_port: 8080,
                known_nodes: vec![],
                private_key_path: PathBuf::from("/etc/rhio/private-key.txt"),
                network_id: "test".into(),
                protocol: None,
            },
            log_level: None,
            publish: Some(PublishConfig {
                s3_buckets: vec![],
                nats_subjects: vec![],
            }),
            subscribe: Some(SubscribeConfig {
                s3_buckets: vec![],
                nats_subjects: vec![],
            }),
        };

        let config: Config = serde_yaml::from_str(&config).expect("valid config.yaml is expected");
        assert_eq!(expected_config, config);
        assert_ne!(hash, "");

        Ok(())
    }

    #[test]
    fn test_full_config() -> Result<()> {
        let rhio_yaml = r#"
        apiVersion: rhio.hiro.io/v1
        kind: RhioService
        metadata:
            name: test-service
            uid: 1
        spec:
            clusterConfig:
                listenerClass: disabled
                gracefulShutdownTimeout: 1s

            image:
                custom: ghcr.io/hiro-microdatacenters-bv/rhio-dev:1.0.1
                productVersion: 1.0.1
                pullPolicy: IfNotPresent      

            configuration:
                networkId: test
                privateKeySecret: secret-key
                nodes: []
                s3: null
                nats:
                    endpoint: nats://nats-jetstream.dkg-engine.svc.cluster.local:4222
                    credentials:

        "#;
        let rhio: RhioService = serde_yaml::from_str(rhio_yaml).expect("illegal test input");

        let config_resources =
            RhioConfigurationResources::from(&rhio, vec![], vec![], vec![], vec![]);

        let labels = build_recommended_labels(&rhio, "controller", "1.0.1", "role", "role_group");
        let (cm, hash) = config_resources
            .build_rhio_configmap(labels)
            .expect("unable to build rhio config");

        let config = cm.data.unwrap().get("config.yaml").unwrap().to_owned();

        let expected_config = Config {
            s3: None,
            nats: NatsConfig {
                endpoint: "nats://nats-jetstream.dkg-engine.svc.cluster.local:4222".into(),
                credentials: None,
            },
            node: NodeConfig {
                bind_port: 9102,
                http_bind_port: 8080,
                known_nodes: vec![],
                private_key_path: PathBuf::from("/etc/rhio/private-key.txt"),
                network_id: "test".into(),
                protocol: None,
            },
            log_level: None,
            publish: Some(PublishConfig {
                s3_buckets: vec![],
                nats_subjects: vec![],
            }),
            subscribe: Some(SubscribeConfig {
                s3_buckets: vec![],
                nats_subjects: vec![],
            }),
        };

        let config: Config = serde_yaml::from_str(&config).unwrap();
        assert_eq!(config, expected_config);

        Ok(())
    }

    fn sample_rms() -> ReplicatedMessageStream {
        let rms_yaml = r#"
        apiVersion: rhio.hiro.io/v1
        kind: ReplicatedMessageStream
        metadata:
            name: test-stream
        spec:
            streamName: test-stream
            subjects:
                - test.subject
          "#;
        let rms: ReplicatedMessageStream = serde_yaml::from_str(rms_yaml).expect("illegal test input");
        rms
    }

    fn sample_rmss() -> ReplicatedMessageStreamSubscription {
        let rmss_yaml = r#"
        apiVersion: rhio.hiro.io/v1
        kind: ReplicatedMessageStreamSubscription
        metadata:
            name: test-stream-subscription
        spec:
            publicKey: b2030d8df6c0a8bc53513e1c1746446ff00424e39f0ba25441f76b3d68752b8c
            subscriptions:
                - subject: test.integration
                stream: test-stream
  
        "#;
        let rmss: ReplicatedMessageStreamSubscription = serde_yaml::from_str(rmss_yaml).expect("illegal test input");
        rmss
    }

    fn sample_ros() -> ReplicatedObjectStore {
        let ros_yaml = r#"
        apiVersion: rhio.hiro.io/v1
        kind: ReplicatedObjectStore
        metadata:
            name: test-store
        spec:
            buckets:
                - source
            "#;
        let ros: ReplicatedObjectStore = serde_yaml::from_str(ros_yaml).expect("illegal test input");
        ros
    }

    fn sample_ross() -> ReplicatedObjectStoreSubscription {
        let ross_yaml = r#"
        apiVersion: rhio.hiro.io/v1
        kind: ReplicatedObjectStoreSubscription
        metadata:
          name: test-store-subscription
        spec:
            publicKey: b2030d8df6c0a8bc53513e1c1746446ff00424e39f0ba25441f76b3d68752b8c
            buckets:
                - remoteBucket: source
                  localBucket: target            
                    "#;
        let ross: ReplicatedObjectStoreSubscription = serde_yaml::from_str(ross_yaml).expect("illegal test input");
        ross
    }

}
