use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::str::FromStr;

use crate::api::container::Container;
use crate::api::message_stream::ReplicatedMessageStream;
use crate::api::message_stream_subscription::ReplicatedMessageStreamSubscription;
use crate::api::object_store::ReplicatedObjectStore;
use crate::api::object_store_subscription::ReplicatedObjectStoreSubscription;
use crate::api::service::{RhioConfig, RhioService};
use crate::rhio_controller::{
    AddVolumeMountSnafu, AddVolumeSnafu, BuildConfigMapSnafu, InvalidAnnotationSnafu,
    InvalidNatsSubjectSnafu, LabelBuildSnafu, MetadataBuildSnafu,
    ObjectMissingMetadataForOwnerRefSnafu, Result, RhioConfigurationSerializationSnafu,
};
use crate::rhio_controller::{InvalidContainerNameSnafu, ObjectMetaSnafu};
use crate::rhio_controller::{APP_NAME, OPERATOR_NAME};
use p2panda_core::PublicKey;
use rhio_config::configuration::{
    Config, KnownNode, LocalNatsSubject, NatsConfig, NatsCredentials, NodeConfig, PublishConfig,
    RemoteNatsSubject, RemoteS3Bucket, S3Config, SubscribeConfig,
};
use s3::creds::Credentials;
use snafu::ResultExt;
use stackable_operator::builder::configmap::ConfigMapBuilder;
use stackable_operator::builder::meta::ObjectMetaBuilder;
use stackable_operator::builder::pod::container::ContainerBuilder;
use stackable_operator::builder::pod::resources::ResourceRequirementsBuilder;
use stackable_operator::builder::pod::PodBuilder;
use stackable_operator::commons::product_image_selection::ResolvedProductImage;
use stackable_operator::k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, ContainerPort, EnvVarSource, ServiceAccount, Volume,
    VolumeMount,
};
use stackable_operator::k8s_openapi::api::core::v1::{HTTPGetAction, Probe, SecretKeySelector};
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use stackable_operator::k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::{Resource, ResourceExt};
use stackable_operator::kvp::consts::STACKABLE_VENDOR_KEY;
use stackable_operator::kvp::{Annotations, Labels, ObjectLabels};
use stackable_operator::role_utils::RoleGroupRef;

pub const RHIO_CONTROLLER_NAME: &str = "rhioservice";
pub const DOCKER_IMAGE_BASE_NAME: &str = "rhio";

pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";
pub const RHIO_CONFIG_DIR: &str = "/etc/rhio/config.yaml";
pub const RHIO_LOG_DIR: &str = "/var/log/rhio";
pub const STACKABLE_VENDOR_VALUE_HIRO: &str = "HIRO";

pub fn build_rhio_statefulset(
    rhio: &RhioService,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<RhioService>,
    service_account: &ServiceAccount,
    config_hash: String,
) -> Result<StatefulSet> {
    let mut container_rhio =
        ContainerBuilder::new(&Container::Rhio.to_string()).context(InvalidContainerNameSnafu {
            name: Container::Rhio.to_string(),
        })?;

    let private_key_name = &rhio.spec.configuration.private_key_secret;

    container_rhio
        .image_from_product_image(resolved_product_image)
        .args(vec![
            "/usr/local/bin/rhio".to_string(),
            "-c".to_string(),
            "/etc/rhio/config.yaml".to_string(),
        ])
        .add_env_var_from_source(
            "PRIVATE_KEY",
            EnvVarSource {
                config_map_key_ref: None,
                field_ref: None,
                resource_field_ref: None,
                secret_key_ref: Some(SecretKeySelector {
                    key: "secretKey".to_owned(),
                    name: private_key_name.to_owned(),
                    optional: None,
                }),
            },
        )
        .add_container_ports(container_ports())
        .add_volume_mounts(vec![VolumeMount {
            name: "config".into(),
            mount_path: "/etc/rhio/config.yaml".into(),
            sub_path: Some("config.yaml".into()),
            ..VolumeMount::default()
        }])
        .context(AddVolumeMountSnafu)?
        .liveness_probe(liveness_probe())
        .readiness_probe(liveness_probe())
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        );

    let mut pod_builder = PodBuilder::new();

    let mut pod_metadata = ObjectMetaBuilder::new()
        .annotations(
            Annotations::try_from([("rhio.hiro.io/config-hash", config_hash)])
                .context(InvalidAnnotationSnafu)?,
        )
        .with_recommended_labels(build_recommended_labels(
            rhio,
            RHIO_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    pod_metadata.labels.get_or_insert(BTreeMap::new()).insert(
        STACKABLE_VENDOR_KEY.into(),
        STACKABLE_VENDOR_VALUE_HIRO.into(),
    );

    pod_builder
        .metadata(pod_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(container_rhio.build())
        // .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rhio.name_any(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any());

    let mut sts_metadata = ObjectMetaBuilder::new()
        .name_and_namespace(rhio)
        .name(rolegroup_ref.object_name())
        .ownerreference_from_resource(rhio, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu { rhio_service: rhio })?
        .with_recommended_labels(build_recommended_labels(
            rhio,
            RHIO_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    sts_metadata.labels.get_or_insert(BTreeMap::new()).insert(
        STACKABLE_VENDOR_KEY.into(),
        STACKABLE_VENDOR_VALUE_HIRO.into(),
    );

    Ok(StatefulSet {
        metadata: sts_metadata,
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        rhio,
                        APP_NAME,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                    .context(LabelBuildSnafu)?
                    .into(),
                ),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: pod_builder.build_template(),
            volume_claim_templates: None,
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

fn liveness_probe() -> Probe {
    Probe {
        failure_threshold: Some(3),
        http_get: Some(HTTPGetAction {
            path: Some("/health".to_owned()),
            port: IntOrString::Int(8080),
            scheme: Some("HTTP".to_owned()),
            ..HTTPGetAction::default()
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        ..Probe::default()
    }
}

fn container_ports() -> Vec<ContainerPort> {
    vec![
        ContainerPort {
            container_port: 8080,
            name: Some("health".into()),
            protocol: Some("TCP".into()),
            ..ContainerPort::default()
        },
        ContainerPort {
            container_port: 9102,
            name: Some("network".into()),
            protocol: Some("UDP".into()),
            ..ContainerPort::default()
        },
    ]
}

pub fn build_recommended_labels<'a>(
    owner: &'a RhioService,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, RhioService> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name,
        role,
        role_group,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_rhio_configmap(
    rhio: &RhioService,
    streams: Vec<ReplicatedMessageStream>,
    stream_subscriptions: Vec<ReplicatedMessageStreamSubscription>,
    stores: Vec<ReplicatedObjectStore>,
    store_subscriptions: Vec<ReplicatedObjectStoreSubscription>,
    owner: &impl Resource<DynamicType = ()>,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<RhioService>,
) -> Result<(ConfigMap, String)> {
    let published_nats_subjects = streams
        .into_iter()
        .flat_map(|stream| {
            stream
                .spec
                .subjects
                .into_iter()
                .map(|published_subject| {
                    rhio_core::Subject::from_str(&published_subject)
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

    let subscribed_subjects = stream_subscriptions
        .into_iter()
        .flat_map(|sub| {
            let public_key = PublicKey::from_str(&sub.spec.public_key).unwrap();
            sub.spec
                .subscriptions
                .into_iter()
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

    let published_buckets = stores
        .into_iter()
        .flat_map(|store| store.spec.buckets.into_iter())
        .collect::<Vec<String>>();

    let subscribed_buckets = store_subscriptions
        .into_iter()
        .flat_map(|sub| {
            let public_key = PublicKey::from_str(&sub.spec.public_key).unwrap();
            sub.spec
                .buckets
                .into_iter()
                .map(|bucket| RemoteS3Bucket {
                    remote_bucket_name: bucket.remote_bucket.to_owned(),
                    local_bucket_name: bucket.local_bucket.to_owned(),
                    public_key,
                })
                .collect::<Vec<RemoteS3Bucket>>()
        })
        .collect::<Vec<RemoteS3Bucket>>();

    let config = build_rhio_configuration(
        &rhio.spec.configuration,
        published_nats_subjects,
        subscribed_subjects,
        published_buckets,
        subscribed_buckets,
    )?;
    let rhio_configuration =
        serde_json::to_string(&config).context(RhioConfigurationSerializationSnafu)?;

    let mut metadata = ObjectMetaBuilder::new()
        .name_and_namespace(rhio)
        .ownerreference_from_resource(owner, None, Some(true))
        .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
            rhio_service: ObjectRef::from_obj(rhio),
        })?
        .with_recommended_labels(build_recommended_labels(
            rhio,
            RHIO_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
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
        .add_data("config.yaml", rhio_configuration)
        .build()
        .context(BuildConfigMapSnafu)?;

    Ok((config_map, config_hash))
}

fn build_rhio_configuration(
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
            bind_port: spec_config.bind_port,
            http_bind_port: spec_config.http_bind_port,
            known_nodes,
            private_key_path: "/etc/rhio/private-key.txt".into(),
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
