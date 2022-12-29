use fluvio::FluvioError;
use http_types::url::ParseError;
use http_types::StatusCode;
use std::io::Error as IoError;
use std::path::PathBuf;
use thiserror::Error;
use toml::de::Error as TomlError;

#[derive(Error, Debug)]
pub enum CloudLoginError {
    #[error("Unable to access the default Fluvio directory")]
    FluvioDirError,
    /// Failed to parse request URL
    #[error("Failed to parse URL: {url_string}")]
    UrlError {
        source: ParseError,
        url_string: String,
    },
    #[error("Failed to make HTTP request to Infinyon cloud")]
    HttpError(#[source] HttpError),
    #[error("Failed to get token from Auth0")]
    FailedToGetAuth0Token,
    #[error("Failed to authenticate with Auth0: {0}")]
    Auth0LoginError(String),
    #[error("Account not found with email provided by third party service, please create account through WEB UI.")]
    Auth0AccountNotFound,
    #[error("Timeout while waiting for user authentication through third party service.")]
    Auth0TimeoutError,
    #[error("Unable to url encode the string")]
    UrlEncode(#[from] serde_urlencoded::ser::Error),
    #[error("Failed to save cloud credentials")]
    UnableToSaveCredentials(#[source] IoError),
    /// Failed to do some IO.
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error("Failed to create logins dir {path}")]
    UnableToCreateLoginsDir { source: IoError, path: PathBuf },
    #[error("Cluster for \"{0}\" does not exist")]
    ClusterDoesNotExist(String),
    #[error("Profile not available yet, please try again later.")]
    ProfileNotAvailable,
    #[error("Failed to parse login token from file")]
    UnableToParseCredentials(#[from] TomlError),
    #[error("Failed to load cloud credentials")]
    UnableToLoadCredentials(#[source] IoError),
    #[error("Failed to download cloud profile: Status code {0}: {1}")]
    ProfileDownloadError(StatusCode, &'static str),
    /// Failed to open Infinyon Cloud login file
    #[error("Not logged in")]
    NotLoggedIn,
    #[error("Fluvio client error")]
    FluvioError(#[from] FluvioError),
    #[error("Failed to authenticate with username: {0}")]
    AuthenticationError(String),
    #[error("Account not active. Please validate email address.")]
    AccountNotActive,
}

#[derive(Error, Debug)]
#[error("An HTTP error occurred: {inner}")]
pub struct HttpError {
    inner: http_types::Error,
}

impl From<http_types::Error> for CloudLoginError {
    fn from(inner: http_types::Error) -> Self {
        Self::HttpError(HttpError { inner })
    }
}
