use std::collections::BTreeMap;
use std::str::FromStr;

use super::rhio_controller::Result;
use crate::api::container::Container;
use crate::api::role::RhioRole;
use crate::api::service::{RhioConfig, RhioService};
use crate::rhio_controller::{
    AddVolumeMountSnafu, AddVolumeSnafu, BuildConfigMapSnafu, Error, LabelBuildSnafu,
    MetadataBuildSnafu, ObjectMissingMetadataForOwnerRefSnafu, RhioConfigurationSerializationSnafu,
};
use crate::rhio_controller::{InvalidContainerNameSnafu, ObjectMetaSnafu};
use crate::rhio_controller::{APP_NAME, OPERATOR_NAME};
use p2panda_core::PublicKey;
use rhio_config::configuration::{Config, KnownNode, NatsConfig, NatsCredentials, NodeConfig};
use snafu::{OptionExt, ResultExt, Snafu};
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
use stackable_operator::kvp::{Labels, ObjectLabels};
use stackable_operator::role_utils::RoleGroupRef;

pub const RHIO_CONTROLLER_NAME: &str = "rhioservice";
pub const DOCKER_IMAGE_BASE_NAME: &str = "rhio";

pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";
pub const RHIO_CONFIG_DIR: &str = "/etc/rhio/config.yaml";
pub const RHIO_LOG_DIR: &str = "/var/log/rhio";
pub const STACKABLE_VENDOR_VALUE_HIRO: &str = "HIRO";

pub fn build_rhio_statefulset(
    rhio: &RhioService,
    // rhio_role: &RhioRole,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<RhioService>,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    // let recommended_object_labels = build_recommended_labels(
    //     rhio,
    //     RHIO_CONTROLLER_NAME,
    //     &resolved_product_image.app_version_label,
    //     &rolegroup_ref.role,
    //     &rolegroup_ref.role_group,
    // );

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

    // let mut metadata = ObjectMetaBuilder::new()
    //     .with_recommended_labels(recommended_object_labels)
    //     .context(MetadataBuildSnafu)?
    //     .build();

    let pod_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(build_recommended_labels(
            rhio,
            RHIO_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    // metadata.labels
    //     .get_or_insert(BTreeMap::new())
    //     .insert(STACKABLE_VENDOR_KEY.into(), STACKABLE_VENDOR_VALUE_HIRO.into());

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

    let sts_metadata = ObjectMetaBuilder::new()
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

    // sts_metadata.labels
    //     .get_or_insert(BTreeMap::new())
    //     .insert(STACKABLE_VENDOR_KEY.into(), STACKABLE_VENDOR_VALUE_HIRO.into());

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

pub fn build_rhio_configmap(
    rhio: &RhioService,
    owner: &impl Resource<DynamicType = ()>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<ConfigMap, Error> {
    let config = build_rhio_configuration(&rhio.spec.configuration)?;
    let rhio_configuration =
        serde_json::to_string(&config).context(RhioConfigurationSerializationSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(rhio)
        .ownerreference_from_resource(owner, None, Some(true))
        .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
            rhio_service: ObjectRef::from_obj(rhio),
        })?
        .with_recommended_labels(build_recommended_labels(
            rhio,
            RHIO_CONTROLLER_NAME,
            &resolved_product_image.product_version,
            &RhioRole::Server.to_string(),
            "discovery",
        ))
        .context(MetadataBuildSnafu)?
        .build();
    ConfigMapBuilder::new()
        .metadata(metadata)
        .add_data("config.yaml", rhio_configuration)
        .build()
        .context(BuildConfigMapSnafu)
}

fn build_rhio_configuration(spec_config: &RhioConfig) -> Result<Config> {
    let known_nodes = spec_config
        .nodes
        .iter()
        .map(|n| KnownNode {
            public_key: PublicKey::from_str(&n.public_key).unwrap().into(),
            direct_addresses: n.endpoints.to_owned(),
        })
        .collect();
    let credentials = spec_config
        .nats
        .clone()
        .unwrap()
        .credentials
        .map(|c| NatsCredentials {
            nkey: None,
            username: Some(c.username.to_owned()),
            password: Some(c.password.to_owned()),
            token: None,
        });
    let nats = NatsConfig {
        endpoint: spec_config.nats.as_ref().unwrap().endpoint.to_owned(),
        credentials,
    };
    let config = Config {
        node: NodeConfig {
            bind_port: spec_config.bind_port,
            http_bind_port: spec_config.http_bind_port,
            known_nodes,
            private_key_path: "/etc/rhio/private-key.txt".into(),
            network_id: spec_config.network_id.to_owned(),
            protocol: None,
        },
        s3: None,
        nats,
        log_level: None,
        publish: None,
        subscribe: None,
    };
    Ok(config)
}
