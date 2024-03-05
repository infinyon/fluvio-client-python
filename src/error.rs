use thiserror::Error;

use fluvio::FluvioError as FluvioClientError;

use crate::PyFluvioError;

#[derive(Error, Debug)]
pub enum FluvioError {
    #[error(transparent)]
    FluvioErr(#[from] FluvioClientError),
    #[error("{0}")]
    AnyhowError(#[from] anyhow::Error),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
}

impl From<FluvioError> for pyo3::PyErr {
    fn from(err: FluvioError) -> pyo3::PyErr {
        PyFluvioError::new_err(err.to_string())
    }
}
