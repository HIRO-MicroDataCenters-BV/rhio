use super::service::RhioService;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::{kube::runtime::reflector::ObjectRef, role_utils::RoleGroupRef};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
pub enum RhioRole {
    #[strum(serialize = "server")]
    Server,
}

// TODO why

impl RhioRole {
    /// Metadata about a rolegroup
    pub fn rolegroup_ref(
        &self,
        service: &RhioService,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<RhioService> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(service),
            role: self.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }
}
