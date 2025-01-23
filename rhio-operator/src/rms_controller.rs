use crate::api::message_stream::ReplicatedMessageStream;
use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    kube::{
        api::DynamicObject,
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
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

    #[snafu(display("object has no name"))]
    ObjectHasNoName,
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
    rhio: Arc<DeserializeGuard<ReplicatedMessageStream>>,
    _ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let _rms = rhio
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidReplicatedMessageStreamSnafu)?;

    Ok(Action::await_change())
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
