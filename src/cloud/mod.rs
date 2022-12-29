mod http;
use http::execute;
mod error;
use async_std::prelude::StreamExt;
pub use error::CloudLoginError;
use fluvio::FluvioConfig;
use fluvio_types::defaults::CLI_CONFIG_PATH;
use http_types::{Mime, Request, Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::{debug, trace};

const DEFAULT_LOGINS_DIR: &str = "logins";
const CURRENT_LOGIN_FILE_NAME: &str = "current";

#[derive(Debug)]
pub struct CloudClient {
    /// Configured path for storing credentials in filesystem
    credentials_path: PathBuf,
}
impl CloudClient {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            credentials_path: path.into(),
        }
    }
    fn default_file_path() -> Result<PathBuf, CloudLoginError> {
        let mut login_path = dirs::home_dir().ok_or(CloudLoginError::FluvioDirError)?;
        login_path.push(CLI_CONFIG_PATH);
        login_path.push(DEFAULT_LOGINS_DIR);
        Ok(login_path)
    }

    pub fn with_default_path() -> Result<Self, CloudLoginError> {
        Ok(Self::new(Self::default_file_path()?))
    }

    pub async fn authenticate_with_auth0(
        &mut self,
        remote: &str,
    ) -> Result<String, CloudLoginError> {
        let mut response = get_auth0_config(remote).await?;

        if response.status() != StatusCode::Ok {
            return Err(CloudLoginError::Auth0LoginError(
                "Auth0 not configured in Fluvio Cloud".into(),
            ));
        }
        let auth0_config = response.body_json::<Auth0Config>().await?;

        let mut response = get_device_code(&auth0_config).await?;
        if response.status() != StatusCode::Ok {
            return Err(CloudLoginError::Auth0LoginError(
                "Failed to get Device Code from Auth0".into(),
            ));
        }

        let device_code = response.body_json::<DeviceCodeResponse>().await?;

        if webbrowser::open(&device_code.verification_uri_complete).is_ok() {
            println!(
                "A web browser has been opened at {}.\nPlease proceed with authentication.",
                device_code.verification_uri_complete
            );
        } else {
            debug!("Failed to open browser to authenticate with Auth0");
            println!(
                "Please visit the following URL: {} and use the following code: {}.\n Then, proceed with authentication.",
                device_code.verification_uri, device_code.user_code
            );
        }

        let mut response = tokio::select!(
             response = get_auth0_token(
                &auth0_config,
                device_code.interval,
                device_code.device_code,
            ) => {response?},
            _ = fluvio_future::timer::sleep(std::time::Duration::from_secs(device_code.expires_in)) => {
                return Err(CloudLoginError::Auth0TimeoutError)
            }
        );
        let token = response.body_json::<TokenResponse>().await?;

        let mut response = authorize_auth0_user(
            remote,
            format!("{} {}", token.token_type, token.access_token),
        )
        .await?;

        match response.status() {
            StatusCode::Ok => {
                let creds = response.body_json::<CredentialsAuth0Response>().await?;
                let email = creds.email;
                println!("Successfully authenticated with Auth0");
                self.save_credentials(remote.to_owned(), email.clone(), creds.inner)
                    .await?;
                Ok(email)
            }
            StatusCode::Forbidden => Err(CloudLoginError::Auth0AccountNotFound),
            _ => {
                debug!(?response, "Failed to login");
                Err(CloudLoginError::Auth0LoginError(
                    "Something went wrong while authenticating with Auth0".into(),
                ))
            }
        }
    }
    pub async fn authenticate(
        &mut self,
        email: &str,
        password: &str,
        remote: &str,
    ) -> Result<(), CloudLoginError> {
        let mut response = login_user(remote, email, password).await?;
        match response.status() {
            StatusCode::Ok => {
                debug!("Successfully authenticated with token");
                let creds = response.body_json::<CredentialsResponse>().await?;
                if let Some(false) = creds.active {
                    return Err(CloudLoginError::AccountNotActive);
                }
                self.save_credentials(remote.to_owned(), email.to_owned(), creds)
                    .await?;
                Ok(())
            }
            _ => {
                debug!(?response, "Failed to login");
                Err(CloudLoginError::AuthenticationError(email.to_owned()))
            }
        }
    }

    async fn save_credentials(
        &mut self,
        remote: String,
        email: String,
        creds: CredentialsResponse,
    ) -> Result<(), CloudLoginError> {
        let creds = creds.into_saved(remote, email);
        // Save credentials to disk
        creds.try_save(&self.credentials_path)?;
        Ok(())
    }

    pub async fn download_profile(&mut self) -> Result<FluvioConfig, CloudLoginError> {
        // Load the current credentials from disk
        let creds = Credentials::try_load(&self.credentials_path, None)?;

        let mut response = download_profile(&creds).await?;
        trace!(?response, "Response");
        debug!(status = response.status() as u16);

        match response.status() {
            StatusCode::Ok => {
                debug!("Profile downloaded");
                let config: FluvioConfig = response.body_json().await?;
                Ok(config)
            }
            StatusCode::NoContent => Err(CloudLoginError::ProfileNotAvailable),
            StatusCode::NotFound => Err(CloudLoginError::ClusterDoesNotExist(creds.email)),
            status => Err(CloudLoginError::ProfileDownloadError(
                status,
                status.canonical_reason(),
            )),
        }
    }
}
async fn get_auth0_config(remote: &str) -> Result<Response, CloudLoginError> {
    let url_string = format!("{}/api/v1/oauth2/auth0/config", remote);
    let url = Url::parse(&url_string)
        .map_err(|source| CloudLoginError::UrlError { source, url_string })?;
    let request = Request::get(url);
    let response = execute(request).await?;
    Ok(response)
}
async fn get_device_code(config: &Auth0Config) -> Result<Response, CloudLoginError> {
    let url_string = format!("https://{}/oauth/device/code", config.domain);
    let url = Url::parse(&url_string)
        .map_err(|source| CloudLoginError::UrlError { source, url_string })?;
    let mut request = Request::post(url);
    let mime = Mime::from_str("application/x-www-form-urlencoded")?;
    request.set_content_type(mime);

    let dv = DeviceCodeRequestBody {
        client_id: config.client_id.to_owned(),
        scope: "openid profile email".into(),
        audience: format!("https://{}/api/v2/", config.domain),
    };

    request.set_body(serde_urlencoded::to_string(dv)?);

    let response = execute(request).await?;
    Ok(response)
}

async fn get_auth0_token(
    config: &Auth0Config,
    interval: u64,
    device_code: String,
) -> Result<Response, CloudLoginError> {
    let token_request_payload = TokenRequestBody {
        client_id: config.client_id.to_owned(),
        device_code,
        grant_type: "urn:ietf:params:oauth:grant-type:device_code".into(),
    };

    let mut interval = async_std::stream::interval(std::time::Duration::from_secs(interval));
    while interval.next().await.is_some() {
        let response = try_get_auth0_token(config, &token_request_payload).await?;
        if response.status() == StatusCode::Ok {
            return Ok(response);
        }
    }
    Err(CloudLoginError::FailedToGetAuth0Token)
}
async fn try_get_auth0_token(
    config: &Auth0Config,
    payload: &TokenRequestBody,
) -> Result<Response, CloudLoginError> {
    let url_string = format!("https://{}/oauth/token", config.domain);
    let url = Url::parse(&url_string)
        .map_err(|source| CloudLoginError::UrlError { source, url_string })?;
    let mut request = Request::post(url);
    let mime = Mime::from_str("application/x-www-form-urlencoded")?;
    request.set_content_type(mime);
    request.set_body(serde_urlencoded::to_string(payload)?);

    let response = execute(request).await?;
    Ok(response)
}

async fn authorize_auth0_user(
    host: &str,
    access_token: String,
) -> Result<Response, CloudLoginError> {
    let url_string = format!("{}/api/v1/oauth2/auth0/authorize?dont_create=1", host);
    let url = Url::parse(&url_string)
        .map_err(|source| CloudLoginError::UrlError { source, url_string })?;
    let mut request = Request::get(url);

    request.append_header("Authorization", &*access_token);

    let response = execute(request).await?;
    Ok(response)
}
async fn download_profile(creds: &Credentials) -> Result<Response, CloudLoginError> {
    let url_string = format!("{}/api/v1/downloadProfile", creds.remote);
    let url = Url::parse(&url_string)
        .map_err(|source| CloudLoginError::UrlError { source, url_string })?;
    let mut request = Request::get(url);
    request.append_header("Authorization", &*creds.token);

    let response = execute(request).await?;
    Ok(response)
}

#[derive(Deserialize, Debug)]
struct Auth0Config {
    domain: String,
    client_id: String,
}

#[derive(Serialize)]
struct DeviceCodeRequestBody {
    client_id: String,
    scope: String,
    audience: String,
}

#[derive(Deserialize)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Serialize)]
struct TokenRequestBody {
    client_id: String,
    device_code: String,
    grant_type: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialsResponse {
    id: String,
    token: String,
    #[serde(skip_serializing, default)]
    active: Option<bool>,
}

impl CredentialsResponse {
    fn into_saved(self, remote: String, email: String) -> Credentials {
        Credentials {
            remote,
            email,
            id: self.id,
            token: self.token,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialsAuth0Response {
    #[serde(flatten)]
    inner: CredentialsResponse,
    email: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Credentials {
    remote: String,
    email: String,
    id: String,
    token: String,
}
impl Credentials {
    /// Try to load credentials from disk
    fn try_load<P: AsRef<Path>>(
        base_path: P,
        key: Option<CredentialKey>,
    ) -> Result<Self, CloudLoginError> {
        let key_md5 = if let Some(key) = key {
            key.md5()
        } else {
            let current_login_path = base_path.as_ref().join(CURRENT_LOGIN_FILE_NAME);
            fs::read_to_string(current_login_path).map_err(|_| CloudLoginError::NotLoggedIn)?
        };

        let cred_path = base_path.as_ref().join(key_md5);

        let file_str =
            fs::read_to_string(cred_path).map_err(CloudLoginError::UnableToLoadCredentials)?;
        let creds: Credentials =
            toml::from_str(&file_str).map_err(CloudLoginError::UnableToParseCredentials)?;
        Ok(creds)
    }
    /// Try to save credentials to disk
    fn try_save<P: AsRef<Path>>(&self, base_path: P) -> Result<(), CloudLoginError> {
        use std::io::Write;
        let base_path = base_path.as_ref();

        // Make sure the logins directory exists
        fs::create_dir_all(base_path).map_err(|source| {
            CloudLoginError::UnableToCreateLoginsDir {
                source,
                path: base_path.to_owned(),
            }
        })?;

        let key = CredentialKey {
            remote: self.remote.clone(),
            email: self.email.clone(),
        };
        let key_md5 = key.md5();
        let cred_path = base_path.join(&key_md5);
        let mut cred_file = fs::File::create(cred_path)?;

        // Serializing self can never fail because Credentials: Serialize
        cred_file
            .write_all(toml::to_string(self).unwrap().as_bytes())
            .map_err(CloudLoginError::UnableToSaveCredentials)?;

        let current_login_path = base_path.join("current");
        let mut current_login_file = fs::File::create(current_login_path)?;

        current_login_file
            .write_all(key_md5.as_bytes())
            .map_err(CloudLoginError::UnableToSaveCredentials)?;

        Ok(())
    }
}
struct CredentialKey {
    remote: String,
    email: String,
}

impl CredentialKey {
    fn md5(&self) -> String {
        use md5::Digest;
        let mut hasher = md5::Md5::new();
        hasher.update(&self.remote);
        hasher.update(&self.email);
        let output = hasher.finalize();
        hex::encode(output)
    }
}

#[derive(Debug, Serialize)]
struct LoginRequest {
    email: String,
    password: String,
}

async fn login_user(
    remote: &str,
    email: &str,
    password: &str,
) -> Result<Response, CloudLoginError> {
    let url_string = format!("{}/api/v1/loginUser", remote);
    let url = Url::parse(&url_string)
        .map_err(|source| CloudLoginError::UrlError { source, url_string })?;
    let mut request = Request::post(url);
    let login = LoginRequest {
        email: email.to_owned(),
        password: password.to_owned(),
    };

    // Always safe to serialize when Self: Serialize
    let body = serde_json::to_string(&login).unwrap();
    request.set_body(body);

    let response = execute(request).await?;
    Ok(response)
}
