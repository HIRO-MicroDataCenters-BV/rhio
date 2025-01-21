#![allow(unused_imports, unused_variables)]
use crate::api::service::RhioService;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use product_config::{
    types::PropertyNameKind,
    writer::{to_java_properties_string, PropertiesWriterError},
    ProductConfigManager,
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference, VolumeBuilder},
            PodBuilder,
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        authentication::AuthenticationClass,
        listener::{Listener, ListenerPort, ListenerSpec},
        opa::OpaApiVersion,
        product_image_selection::ResolvedProductImage,
        rbac::build_rbac_resources,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, ContainerPort, EnvVar,
                EnvVarSource, ExecAction, ObjectFieldSelector, PodSpec, Probe, Service,
                ServiceAccount, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        DeepMerge,
    },
    kube::{
        api::DynamicObject,
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    kvp::{Label, Labels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        framework::LoggingError,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::{GenericRoleConfig, RoleGroupRef},
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no name"))]
    ObjectHasNoName,

    #[snafu(display("invalid rhio service"))]
    InvalidRhioService,
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectHasNoName => None,
            _ => None,
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_rhio(
    rhio: Arc<DeserializeGuard<RhioService>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");
    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<RhioService>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidRhioService { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}
