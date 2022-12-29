use async_h1::client;
use http_types::{headers::USER_AGENT, Error, Request, Response, StatusCode};
use tracing::debug;

pub const FLUVIO_CLI_USER_AGENT: &str = "Fluvio Python Client";

pub async fn execute(req: Request) -> Result<Response, Error> {
    debug!(?req, "executing request");
    let mut req = req;
    let host = req
        .url()
        .host_str()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing hostname"))?
        .to_string();

    let is_https = req.url().scheme() == "https";

    let addr = req
        .url()
        .socket_addrs(|| if is_https { Some(443) } else { Some(80) })?
        .into_iter()
        .next()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing valid address"))?;

    let tcp_stream = fluvio_future::net::TcpStream::connect(addr).await?;

    req.append_header(
        USER_AGENT,
        format!("{}/{}", FLUVIO_CLI_USER_AGENT, env!("CARGO_PKG_VERSION")),
    );

    let result = if is_https {
        let tls_connector = fluvio_future::native_tls::TlsConnector::default();
        let tls_stream = tls_connector.connect(host, tcp_stream).await?;
        client::connect(tls_stream, req).await
    } else {
        client::connect(tcp_stream, req).await
    };

    debug!(?result, "http result");
    result
}
