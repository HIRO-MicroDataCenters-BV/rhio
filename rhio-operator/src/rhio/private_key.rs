use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivateKey {
    pub secret_key: String,
}
