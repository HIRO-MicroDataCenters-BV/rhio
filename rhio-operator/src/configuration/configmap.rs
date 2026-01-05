use crate::configuration::error::Result;
use crate::rhio::builders::STACKABLE_VENDOR_VALUE_HIRO;
use crate::{
    api::{
        message_stream::ReplicatedMessageStream,
        message_stream_subscription::ReplicatedMessageStreamSubscription,
        object_store::ReplicatedObjectStore,
        object_store_subscription::ReplicatedObjectStoreSubscription,
        service::{RhioConfig, RhioService},
    },
    configuration::error::{
        BuildConfigMapSnafu, InvalidNatsSubjectSnafu, MetadataBuildSnafu,
        ObjectMissingMetadataForOwnerRefSnafu, RhioConfigurationSerializationSnafu,
    },
};
use p2panda_core::PublicKey;
use rhio_config::configuration::{
    Config, KnownNode, LocalNatsSubject, NatsConfig, NatsCredentials, NodeConfig, PublishConfig,
    RemoteNatsSubject, RemoteS3Bucket, S3Config, SubscribeConfig,
};
use s3::creds::Credentials;
use snafu::ResultExt;
use stackable_operator::k8s_openapi::api::core::v1::ConfigMap;
use stackable_operator::kube::api::ObjectMeta;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kvp::ObjectLabels;
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    kvp::consts::STACKABLE_VENDOR_KEY,
};
use std::hash::{Hash, Hasher};
use std::{collections::BTreeMap, hash::DefaultHasher, str::FromStr};

use super::error::PublicKeyParsingSnafu;
use super::secret::Secret;

pub type ConfigHash = String;

const RHIO_PRIVATE_KEY_PATH: &str = "/etc/rhio/private-key.txt";
pub const RHIO_BIND_PORT_DEFAULT: u16 = 9102;
pub const RHIO_BIND_HTTP_PORT_DEFAULT: u16 = 8080;
pub const RHIO_CONFIG_MAP_ENTRY: &str = "config.yaml";

/// `RhioConfigMapBuilder` is a builder for creating Kubernetes ConfigMap that contains the
/// configuration for a `RhioService`.
///
/// It aggregates various configuration sources such as NATS subjects, S3 buckets, and credentials,
/// and serializes them into a YAML configuration file stored in the ConfigMap.
///
/// # Fields
///
/// - `rhio`: The `RhioService` instance containing the main configuration.
/// - `rms`: A vector of `ReplicatedMessageStream` instances representing the message streams to be published.
/// - `rmss`: A vector of `ReplicatedMessageStreamSubscription` instances representing the message stream subscriptions.
/// - `ros`: A vector of `ReplicatedObjectStore` instances representing the object stores to be published.
/// - `ross`: A vector of `ReplicatedObjectStoreSubscription` instances representing the object store subscriptions.
/// - `nats_secret`: An optional `Secret` containing NATS credentials.
/// - `s3_secret`: An optional `Secret` containing S3 credentials.
///
/// # Methods
///
/// - `from`: Constructs a new `RhioConfigMapBuilder` from the given parameters.
/// - `build`: Builds the ConfigMap and returns it along with a hash of the configuration.
///
/// - `published_nats_subjects`: Retrieves the list of NATS subjects to be published.
/// - `subscribed_nats_subjects`: Retrieves the list of NATS subjects to be subscribed to.
/// - `subscribed_buckets`: Retrieves the list of S3 buckets to be subscribed to.
/// - `published_buckets`: Retrieves the list of S3 buckets to be published.
/// - `nats_credentials`: Retrieves the NATS credentials from the secret.
/// - `s3_credentials`: Retrieves the S3 credentials from the secret.
/// - `build_configmap_metadata`: Builds the metadata for the ConfigMap, including labels and owner references.
/// - `build_config_yaml`: Constructs the YAML configuration file from the various configuration sources.
///
pub struct RhioConfigMapBuilder {
    rhio: RhioService,
    rms: Vec<ReplicatedMessageStream>,
    rmss: Vec<ReplicatedMessageStreamSubscription>,
    ros: Vec<ReplicatedObjectStore>,
    ross: Vec<ReplicatedObjectStoreSubscription>,
    nats_secret: Option<Secret<NatsCredentials>>,
    s3_secret: Option<Secret<Credentials>>,
}

impl RhioConfigMapBuilder {
    pub fn from(
        rhio: RhioService,
        rms: Vec<ReplicatedMessageStream>,
        rmss: Vec<ReplicatedMessageStreamSubscription>,
        ros: Vec<ReplicatedObjectStore>,
        ross: Vec<ReplicatedObjectStoreSubscription>,
        nats_secret: Option<Secret<NatsCredentials>>,
        s3_secret: Option<Secret<Credentials>>,
    ) -> RhioConfigMapBuilder {
        RhioConfigMapBuilder {
            rms,
            rmss,
            ros,
            ross,
            rhio,
            nats_secret,
            s3_secret,
        }
    }

    pub fn build(&self, labels: ObjectLabels<RhioService>) -> Result<(ConfigMap, ConfigHash)> {
        let published_subjects = self.published_nats_subjects()?;
        let subscribed_subjects = self.subscribed_nats_subjects()?;
        let published_buckets = self.published_buckets();
        let subscribed_buckets = self.subscribed_buckets()?;
        let nats_credentials = self.nats_credentials();
        let s3_credentials = self.s3_credentials();

        let config = self.build_config_yaml(
            &self.rhio.spec.configuration,
            nats_credentials,
            s3_credentials,
            published_subjects,
            subscribed_subjects,
            published_buckets,
            subscribed_buckets,
        )?;
        let mut hasher = DefaultHasher::new();
        config.hash(&mut hasher);
        let config_hash = hasher.finish().to_string();

        let metadata = self.build_configmap_metadata(labels)?;
        let config_map = ConfigMapBuilder::new()
            .metadata(metadata)
            .add_data(RHIO_CONFIG_MAP_ENTRY, config)
            .build()
            .context(BuildConfigMapSnafu)?;

        Ok((config_map, config_hash))
    }

    fn published_nats_subjects(&self) -> Result<Vec<LocalNatsSubject>> {
        let published_nats_subjects = self
            .rms
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
        Ok(published_nats_subjects)
    }

    fn subscribed_nats_subjects(&self) -> Result<Vec<RemoteNatsSubject>> {
        let subscribed_subjects = self
            .rmss
            .iter()
            .flat_map(|sub| {
                let maybe_public_key =
                    PublicKey::from_str(&sub.spec.public_key).context(PublicKeyParsingSnafu {
                        key: sub.spec.public_key.to_owned(),
                    });

                let public_key = match maybe_public_key {
                    Ok(key) => key,
                    Err(error) => return vec![Err(error)],
                };
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
        Ok(subscribed_subjects)
    }

    fn subscribed_buckets(&self) -> Result<Vec<RemoteS3Bucket>> {
        self.ross
            .iter()
            .flat_map(|sub| {
                let maybe_public_key =
                    PublicKey::from_str(&sub.spec.public_key).context(PublicKeyParsingSnafu {
                        key: sub.spec.public_key.to_owned(),
                    });

                let public_key = match maybe_public_key {
                    Ok(key) => key,
                    Err(error) => return vec![Err(error)],
                };

                sub.spec
                    .buckets
                    .iter()
                    .map(|bucket| {
                        Ok(RemoteS3Bucket {
                            remote_bucket_name: bucket.remote_bucket.to_owned(),
                            local_bucket_name: bucket.local_bucket.to_owned(),
                            public_key,
                        })
                    })
                    .collect::<Vec<Result<RemoteS3Bucket>>>()
            })
            .collect::<Result<Vec<RemoteS3Bucket>>>()
    }

    fn published_buckets(&self) -> Vec<String> {
        self.ros
            .iter()
            .flat_map(|store| store.spec.buckets.iter())
            .cloned()
            .collect::<Vec<String>>()
    }

    fn nats_credentials(&self) -> Option<NatsCredentials> {
        self.nats_secret.as_ref().map(|s| s.value().to_owned())
    }

    fn s3_credentials(&self) -> Option<Credentials> {
        self.s3_secret.as_ref().map(|s| s.value().to_owned())
    }

    fn build_configmap_metadata(
        &self,
        labels: ObjectLabels<'_, RhioService>,
    ) -> Result<ObjectMeta> {
        let mut metadata = ObjectMetaBuilder::new()
            .name_and_namespace(&self.rhio)
            .ownerreference_from_resource(&self.rhio, None, Some(true))
            .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                rhio: ObjectRef::from_obj(&self.rhio),
            })?
            .with_recommended_labels(labels)
            .context(MetadataBuildSnafu)?
            .build();

        metadata.labels.get_or_insert(BTreeMap::new()).insert(
            STACKABLE_VENDOR_KEY.into(),
            STACKABLE_VENDOR_VALUE_HIRO.into(),
        );
        Ok(metadata)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_config_yaml(
        &self,
        service_spec: &RhioConfig,
        nats_credentials: Option<NatsCredentials>,
        s3_credentials: Option<Credentials>,
        nats_subjects: Vec<LocalNatsSubject>,
        subscribe_subjects: Vec<RemoteNatsSubject>,
        s3_buckets: Vec<String>,
        subscribe_buckets: Vec<RemoteS3Bucket>,
    ) -> Result<String> {
        let known_nodes = service_spec
            .nodes
            .iter()
            .map(|n| {
                let public_key =
                    PublicKey::from_str(&n.public_key).context(PublicKeyParsingSnafu {
                        key: n.public_key.to_owned(),
                    })?;

                Ok(KnownNode {
                    public_key,
                    direct_addresses: n.endpoints.to_owned(),
                })
            })
            .collect::<Result<Vec<KnownNode>>>()?;

        let nats = NatsConfig {
            endpoint: service_spec.nats.endpoint.to_owned(),
            credentials: nats_credentials,
        };

        let s3 = service_spec.s3.as_ref().map(|s3_conf| S3Config {
            endpoint: s3_conf.endpoint.to_owned(),
            region: s3_conf.region.to_owned(),
            credentials: s3_credentials,
            watcher_poll_interval_millis: None,
        });
        let config = Config {
            node: NodeConfig {
                bind_port: RHIO_BIND_PORT_DEFAULT,
                http_bind_port: RHIO_BIND_HTTP_PORT_DEFAULT,
                known_nodes,
                private_key_path: RHIO_PRIVATE_KEY_PATH.into(),
                network_id: service_spec.network_id.to_owned(),
                protocol: None,
                discovery: None,
            },
            s3,
            nats,
            log_level: service_spec.log_level.to_owned(),
            publish: Some(PublishConfig {
                s3_buckets,
                nats_subjects,
            }),
            subscribe: Some(SubscribeConfig {
                s3_buckets: subscribe_buckets,
                nats_subjects: subscribe_subjects,
            }),
        };

        let rhio_configuration =
            serde_json::to_string(&config).context(RhioConfigurationSerializationSnafu)?;
        Ok(rhio_configuration)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;

    use p2panda_core::PublicKey;
    use rhio_config::configuration::{
        Config, KnownNode, LocalNatsSubject, NatsConfig, NatsCredentials, NodeConfig,
        PublishConfig, RemoteNatsSubject, RemoteS3Bucket, S3Config, SubscribeConfig,
    };
    use rhio_core::Subject;
    use s3::creds::Credentials;

    use super::RhioConfigMapBuilder;
    use super::*;
    use crate::api::message_stream::ReplicatedMessageStream;
    use crate::api::message_stream_subscription::ReplicatedMessageStreamSubscription;
    use crate::api::object_store::ReplicatedObjectStore;
    use crate::api::object_store_subscription::ReplicatedObjectStoreSubscription;
    use crate::configuration::fixtures;
    use crate::{api::service::RhioService, rhio::builders::build_recommended_labels};

    #[test]
    fn test_minimal_config() {
        let rhio: RhioService =
            serde_yaml_bw::from_str(fixtures::minimal::RHIO).expect("illegal input yaml");

        let config_resources =
            RhioConfigMapBuilder::from(rhio.clone(), vec![], vec![], vec![], vec![], None, None);

        let labels = build_recommended_labels(&rhio, "controller", "1.0.1", "role", "role_group");
        let (cm, hash) = config_resources
            .build(labels)
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
                discovery: None,
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

        let config: Config =
            serde_yaml_bw::from_str(&config).expect("valid config.yaml is expected");
        assert_eq!(expected_config, config);
        assert_ne!(hash, "");
    }

    #[test]
    fn test_full_config() {
        let rhio: RhioService =
            serde_yaml_bw::from_str(fixtures::full::RHIO).expect("illegal input rhio service");
        let rhio_nats: stackable_operator::k8s_openapi::api::core::v1::Secret =
            serde_yaml_bw::from_str(fixtures::full::RHIO_NATS)
                .expect("illegal input nats credentials secret");
        let rhio_s3: stackable_operator::k8s_openapi::api::core::v1::Secret =
            serde_yaml_bw::from_str(fixtures::full::RHIO_S3)
                .expect("illegal input s3 credentials secret");

        let rms: ReplicatedMessageStream =
            serde_yaml_bw::from_str(fixtures::full::RMS).expect("illegal input rms");
        let rmss: ReplicatedMessageStreamSubscription =
            serde_yaml_bw::from_str(fixtures::full::RMSS).expect("illegal input rmss");
        let ros: ReplicatedObjectStore =
            serde_yaml_bw::from_str(fixtures::full::ROS).expect("illegal input ros");
        let ross: ReplicatedObjectStoreSubscription =
            serde_yaml_bw::from_str(fixtures::full::ROSS).expect("illegal input ross");

        let config_resources = RhioConfigMapBuilder::from(
            rhio.clone(),
            vec![rms],
            vec![rmss],
            vec![ros],
            vec![ross],
            Some(Secret::from(rhio_nats).expect("invalid nats secret")),
            Some(Secret::from(rhio_s3).expect("invalid s3 secret")),
        );

        let labels = build_recommended_labels(&rhio, "controller", "1.0.1", "role", "role_group");
        let (cm, hash) = config_resources
            .build(labels)
            .expect("unable to build rhio config");

        let config = cm.data.unwrap().get("config.yaml").unwrap().to_owned();

        let expected_config = Config {
            s3: Some(S3Config {
                endpoint: "http://localhost:32000".into(),
                region: "eu-west-01".into(),
                credentials: Some(Credentials {
                    access_key: Some("access-key".into()),
                    secret_key: Some("secret-key".into()),
                    security_token: None,
                    session_token: None,
                    expiration: None,
                }),
                watcher_poll_interval_millis: None,
            }),
            nats: NatsConfig {
                endpoint: "nats://nats-jetstream.dkg-engine.svc.cluster.local:4222".into(),
                credentials: Some(NatsCredentials {
                    nkey: None,
                    username: Some("username".into()),
                    password: Some("password".into()),
                    token: None,
                }),
            },
            node: NodeConfig {
                bind_port: 9102,
                http_bind_port: 8080,
                known_nodes: vec![KnownNode {
                    public_key: PublicKey::from_str(
                        "b2030d8df6c0a8bc53513e1c1746446ff00424e39f0ba25441f76b3d68752b8c",
                    )
                    .unwrap(),
                    direct_addresses: vec!["10.0.1.2:9102".into(), "10.0.1.3:9102".into()],
                }],
                private_key_path: PathBuf::from("/etc/rhio/private-key.txt"),
                network_id: "test".into(),
                protocol: None,
                discovery: None,
            },
            log_level: Some("=INFO".into()),
            publish: Some(PublishConfig {
                s3_buckets: vec!["source".into()],
                nats_subjects: vec![LocalNatsSubject {
                    subject: Subject::from_str("test.subject").unwrap(),
                    stream_name: "test-stream".into(),
                }],
            }),
            subscribe: Some(SubscribeConfig {
                s3_buckets: vec![RemoteS3Bucket {
                    remote_bucket_name: "source".into(),
                    local_bucket_name: "target".into(),
                    public_key: PublicKey::from_str(
                        "b2030d8df6c0a8bc53513e1c1746446ff00424e39f0ba25441f76b3d68752b8c",
                    )
                    .unwrap(),
                }],
                nats_subjects: vec![RemoteNatsSubject {
                    subject: Subject::from_str("test.subject").unwrap(),
                    stream_name: "test-stream".into(),
                    public_key: PublicKey::from_str(
                        "b2030d8df6c0a8bc53513e1c1746446ff00424e39f0ba25441f76b3d68752b8c",
                    )
                    .unwrap(),
                }],
            }),
        };

        let config: Config = serde_yaml_bw::from_str(&config).expect("fail to deserialize config");
        assert_eq!(config, expected_config);
        assert_ne!(hash, "");
    }

    #[test]
    fn test_metadata() {
        let rhio: RhioService =
            serde_yaml_bw::from_str(fixtures::minimal::RHIO).expect("illegal input yaml");

        let config_resources =
            RhioConfigMapBuilder::from(rhio.clone(), vec![], vec![], vec![], vec![], None, None);

        let labels = build_recommended_labels(&rhio, "controller", "1.0.1", "role", "role_group");
        let (cm, _) = config_resources
            .build(labels)
            .expect("unable to build rhio config");
        let metadata = cm.metadata;

        let labels = vec![
            ("app.kubernetes.io/component", "role"),
            ("app.kubernetes.io/instance", "test-service"),
            ("app.kubernetes.io/managed-by", "rhio.hiro.io_controller"),
            ("app.kubernetes.io/name", "rhio"),
            ("app.kubernetes.io/role-group", "role_group"),
            ("app.kubernetes.io/version", "1.0.1"),
            ("stackable.tech/vendor", "HIRO"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<BTreeMap<String, String>>();

        assert_eq!(metadata.labels.as_ref().unwrap(), &labels);
    }
}
