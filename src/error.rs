use thiserror::Error;

use fluvio::FluvioError as FluvioClientError;

#[derive(Error, Debug)]
pub enum FluvioError {
    #[error(transparent)]
    FluvioError(#[from] FluvioClientError),
    #[error("{0}")]
    AnyhowError(#[from] anyhow::Error),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
}
