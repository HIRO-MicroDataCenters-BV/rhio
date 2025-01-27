use crate::{
    api::{
        message_stream::{ReplicatedMessageStream, ReplicatedMessageStreamStatus},
        service::RhioService,
    },
    rhio_controller::OPERATOR_NAME,
};
use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    kube::{
        api::{DynamicObject, ListParams},
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        ResourceExt,
    },
    logging::controller::ReconcilerError,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("ReplicatedMessageStream object is invalid"))]
    InvalidReplicatedMessageStream {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("object has no name"))]
    ObjectHasNoName,

    #[snafu(display("failed to get rhio services"))]
    GetRhioService {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to get rhio services"))]
    RhioIsAbsent,

    #[snafu(display("Unable to contact RhioService"))]
    UnableToContactRhioService { source: reqwest::Error },

    #[snafu(display("Rhio Service has no status"))]
    RhioServiceHasNoStatus,

    #[snafu(display("Rhio Service has no status"))]
    RhioServiceHasNoStatusForStream,

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
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

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_rms(
    rms_object: Arc<DeserializeGuard<ReplicatedMessageStream>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let rms = rms_object
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidReplicatedMessageStreamSnafu)?;

    let client = &ctx.client;

    let services = client
        .list::<RhioService>(
            rms.namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
            &ListParams::default(),
        )
        .await
        .context(GetRhioServiceSnafu)?
        .into_iter()
        .filter(|stream| stream.metadata.namespace == rms.metadata.namespace)
        .collect::<Vec<RhioService>>();

    let service = services.first().context(RhioIsAbsentSnafu)?;
    let service_status = service
        .status
        .as_ref()
        .context(RhioServiceHasNoStatusSnafu)?;
    let statuses = service_status.status_for(rms);

    let status = ReplicatedMessageStreamStatus { subjects: statuses };

    client
        .apply_patch_status(OPERATOR_NAME, rms, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::requeue(*Duration::from_secs(60)))
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<ReplicatedMessageStream>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidReplicatedMessageStream { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}
