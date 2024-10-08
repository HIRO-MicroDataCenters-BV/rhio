#[derive(Debug, thiserror::Error, uniffi::Object)]
#[error("{e:?}")]
#[uniffi::export(Debug)]
pub struct RhioError {
    e: anyhow::Error,
}

#[uniffi::export]
impl RhioError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl From<anyhow::Error> for RhioError {
    fn from(e: anyhow::Error) -> Self {
        Self { e }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, uniffi::Error)]
pub enum CallbackError {
    #[error("Callback failed")]
    Error,
}

impl From<CallbackError> for RhioError {
    fn from(e: CallbackError) -> Self {
        RhioError {
            e: anyhow::anyhow!("{:?}", e),
        }
    }
}

impl From<anyhow::Error> for CallbackError {
    fn from(_e: anyhow::Error) -> Self {
        CallbackError::Error
    }
}

impl From<uniffi::UnexpectedUniFFICallbackError> for CallbackError {
    fn from(_: uniffi::UnexpectedUniFFICallbackError) -> Self {
        CallbackError::Error
    }
}
