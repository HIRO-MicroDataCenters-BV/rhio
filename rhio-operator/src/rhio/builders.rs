use std::collections::BTreeMap;

use crate::api::service::RhioService;
use crate::configuration::configmap::{
    RHIO_BIND_HTTP_PORT_DEFAULT, RHIO_BIND_PORT_DEFAULT, RHIO_CONFIG_MAP_ENTRY,
};
use crate::operations::graceful_shutdown::add_graceful_shutdown_config;
use crate::rhio::controller::{APP_NAME, OPERATOR_NAME};
use crate::rhio::error::{
    AddVolumeMountSnafu, AddVolumeSnafu, GlobalServiceNameNotFoundSnafu, GracefulShutdownSnafu,
    InvalidAnnotationSnafu, ObjectMissingMetadataForOwnerRefSnafu, Result,
};
use snafu::{OptionExt, ResultExt};
use stackable_operator::builder::meta::ObjectMetaBuilder;
use stackable_operator::builder::pod::container::ContainerBuilder;
use stackable_operator::builder::pod::resources::ResourceRequirementsBuilder;
use stackable_operator::builder::pod::PodBuilder;
use stackable_operator::commons::product_image_selection::ResolvedProductImage;
use stackable_operator::k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, ContainerPort, EnvVarSource, Service, ServiceAccount, ServicePort,
    ServiceSpec, Volume, VolumeMount,
};
use stackable_operator::k8s_openapi::api::core::v1::{HTTPGetAction, Probe, SecretKeySelector};
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use stackable_operator::k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use stackable_operator::kube::ResourceExt;
use stackable_operator::kvp::consts::STACKABLE_VENDOR_KEY;
use stackable_operator::kvp::{Annotations, Labels, ObjectLabels};
use stackable_operator::role_utils::RoleGroupRef;

use super::error::{InvalidContainerNameSnafu, ObjectMetaSnafu};

pub const RHIO_CONFIG_DIR: &str = "/etc/rhio/config.yaml";
pub const RHIO_CONFIG_VOLUME_NAME: &str = "config";
pub const RHIO_LOG_DIR: &str = "/var/log/rhio";
pub const STACKABLE_VENDOR_VALUE_HIRO: &str = "HIRO";
pub const CONTAINER: &str = "rhio";
pub const RHIO_POD_TEMPLATE_CONFIG_HASH: &str = "rhio.hiro.io/config-hash";
const RHIO_BINARY: &str = "/usr/local/bin/rhio";
const RHIO_PRIVATE_KEY_BINARY_VARIABLE: &str = "PRIVATE_KEY";

pub fn build_statefulset(
    rhio: &RhioService,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<RhioService>,
    labels: ObjectLabels<RhioService>,
    role_group_selector: Labels,
    service_account: &ServiceAccount,
    config_hash: String,
) -> Result<StatefulSet> {
    let mut container_rhio =
        ContainerBuilder::new(CONTAINER).context(InvalidContainerNameSnafu {
            name: CONTAINER.to_string(),
        })?;

    let private_key_name = &rhio.spec.configuration.private_key_secret;

    container_rhio
        .image_from_product_image(resolved_product_image)
        .args(vec![
            RHIO_BINARY.to_string(),
            "-c".to_string(),
            RHIO_CONFIG_DIR.to_string(),
        ])
        .add_env_var_from_source(
            RHIO_PRIVATE_KEY_BINARY_VARIABLE,
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
            name: RHIO_CONFIG_VOLUME_NAME.into(),
            mount_path: RHIO_CONFIG_DIR.into(),
            sub_path: Some(RHIO_CONFIG_MAP_ENTRY.into()),
            ..VolumeMount::default()
        }])
        .context(AddVolumeMountSnafu)?
        .liveness_probe(liveness_probe())
        .readiness_probe(liveness_probe())
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("1")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        );

    let mut pod_builder = PodBuilder::new();

    let mut pod_metadata = ObjectMetaBuilder::new()
        .annotations(
            Annotations::try_from([(RHIO_POD_TEMPLATE_CONFIG_HASH, config_hash)])
                .context(InvalidAnnotationSnafu)?,
        )
        .with_recommended_labels(labels.clone())
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
        .add_volume(Volume {
            name: RHIO_CONFIG_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rhio.name_any(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any());

    if let Some(affinity) = &rhio.spec.cluster_config.affinity {
        pod_builder.affinity(affinity);
    }

    let mut sts_metadata = ObjectMetaBuilder::new()
        .name_and_namespace(rhio)
        .name(rolegroup_ref.object_name())
        .ownerreference_from_resource(rhio, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu { rhio_service: rhio })?
        .with_recommended_labels(labels.clone())
        .context(ObjectMetaSnafu)?
        .build();

    sts_metadata.labels.get_or_insert(BTreeMap::new()).insert(
        STACKABLE_VENDOR_KEY.into(),
        STACKABLE_VENDOR_VALUE_HIRO.into(),
    );

    add_graceful_shutdown_config(&rhio.spec.cluster_config, &mut pod_builder)
        .context(GracefulShutdownSnafu)?;

    Ok(StatefulSet {
        metadata: sts_metadata,
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(role_group_selector.into()),
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
            port: IntOrString::Int(RHIO_BIND_HTTP_PORT_DEFAULT as i32),
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
            container_port: RHIO_BIND_HTTP_PORT_DEFAULT as i32,
            name: Some("health".into()),
            protocol: Some("TCP".into()),
            ..ContainerPort::default()
        },
        ContainerPort {
            container_port: RHIO_BIND_PORT_DEFAULT as i32,
            name: Some("rhio".into()),
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

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
///
pub fn build_server_role_service(
    rhio: &RhioService,
    recommended_labels: ObjectLabels<RhioService>,
    service_selector_labels: Labels,
) -> Result<Service> {
    let role_svc_name = rhio
        .server_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(rhio)
        .name(&role_svc_name)
        .ownerreference_from_resource(rhio, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu { rhio_service: rhio })?
        .with_recommended_labels(recommended_labels)
        .context(ObjectMetaSnafu)?
        .build();

    let service_spec = ServiceSpec {
        ports: Some(vec![
            ServicePort {
                name: Some("rhio".to_string()),
                port: RHIO_BIND_PORT_DEFAULT as i32,
                protocol: Some("UDP".to_string()),
                ..ServicePort::default()
            },
            ServicePort {
                name: Some("health".to_string()),
                port: RHIO_BIND_HTTP_PORT_DEFAULT as i32,
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            },
        ]),
        selector: Some(service_selector_labels.into()),
        type_: Some(rhio.spec.cluster_config.listener_class.k8s_service_type()),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        ..Service::default()
    })
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_build_statefulset() {}

    #[test]
    fn test_build_build_server_role_service() {}
}
