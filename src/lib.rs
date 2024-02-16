#![allow(non_snake_case, unused)]
use fluvio::config::{ConfigFile, Profile, TlsCerts, TlsConfig, TlsPaths, TlsPolicy};
use fluvio::consumer::{
    ConsumerConfig as NativeConsumerConfig, ConsumerConfigBuilder,
    SmartModuleContextData as NativeSmartModuleContextData, SmartModuleExtraParams,
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind as NativeSmartModuleKind,
};
use fluvio::dataplane::link::ErrorCode;
use fluvio::{FluvioConfig as NativeFluvioConfig, PartitionSelectionStrategy as NativePartitionSelectionStrategy};
use fluvio::{
    consumer::Record as NativeRecord, Fluvio as NativeFluvio,
    MultiplePartitionConsumer as NativeMultiplePartitionConsumer, Offset as NativeOffset,
    PartitionConsumer as NativePartitionConsumer, TopicProducer as NativeTopicProducer,
};
use fluvio_future::{
    io::{Stream, StreamExt},
    task::run_block_on,
};
use fluvio_types::PartitionId;
use futures::future::BoxFuture;
use futures::pin_mut;
use futures::TryFutureExt;
use std::io::{self, Write};
use std::pin::Pin;
use std::string::FromUtf8Error;
use std::sync::Arc;
use tracing::info;
use url::Host;
mod cloud;
// use crate::error::FluvioError;
mod error;
use cloud::{CloudClient, CloudLoginError};
use error::FluvioError;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

pyo3::create_exception!(mymodule, PyFluvioError, PyException);

#[pymodule]
fn _fluvio_python(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Fluvio>()?;
    m.add_class::<FluvioConfig>()?;
    m.add_class::<ConsumerConfig>()?;
    m.add_class::<PartitionConsumer>()?;
    m.add_class::<PartitionConsumerStream>()?;
    m.add_class::<TopicProducer>()?;
    m.add_class::<ProducerBatchRecord>()?;
    m.add_class::<SmartModuleKind>()?;
    m.add_class::<Record>()?;
    m.add_class::<Offset>()?;
    m.add_class::<Cloud>()?;
    m.add_class::<MultiplePartitionConsumer>()?;
    m.add_class::<PartitionSelectionStrategy>()?;
    m.add("Error", py.get_type::<PyFluvioError>())?;
    Ok(())
}

fn utf8_to_py_err(err: FromUtf8Error) -> PyErr {
    PyFluvioError::new_err(err.to_string())
}
fn error_to_py_err(err: anyhow::Error) -> PyErr {
    PyFluvioError::new_err(err.to_string())
}

#[pyclass]
struct Fluvio(NativeFluvio);

#[pymethods]
impl Fluvio {
    #[staticmethod]
    fn connect() -> PyResult<Fluvio> {
        Ok(Fluvio(
            run_block_on(NativeFluvio::connect()).map_err(error_to_py_err)?,
        ))
    }

    #[staticmethod]
    fn connect_with_config(config: &FluvioConfig) -> PyResult<Fluvio> {
        Ok(Fluvio(
            run_block_on(NativeFluvio::connect_with_config(&config.inner))
                .map_err(error_to_py_err)?,
        ))
    }

    fn partition_consumer(&self, topic: String, partition: u32) -> PyResult<PartitionConsumer> {
        Ok(PartitionConsumer(
            run_block_on(self.0.partition_consumer(topic, partition)).map_err(error_to_py_err)?,
        ))
    }

    fn multi_partition_consumer(&self, strategy: PartitionSelectionStrategy) -> PyResult<MultiplePartitionConsumer> {
        Ok(MultiplePartitionConsumer(
            run_block_on(self.0.consumer(strategy.into_inner())).map_err(error_to_py_err)?,
        ))
    }

    fn topic_producer(&self, topic: String) -> PyResult<TopicProducer> {
        Ok(TopicProducer(
            run_block_on(self.0.topic_producer(topic)).map_err(error_to_py_err)?,
        ))
    }
}

#[derive(Clone)]
#[pyclass]
pub struct PartitionSelectionStrategy {
    inner: NativePartitionSelectionStrategy,
}

impl  PartitionSelectionStrategy {
    fn into_inner(self) -> NativePartitionSelectionStrategy {
        self.inner
    }
}

#[pymethods]
impl  PartitionSelectionStrategy {
    #[staticmethod]
    fn with_all(topic: &str) -> Self {
        Self {
            inner: NativePartitionSelectionStrategy::All(topic.to_owned()),
        }
    }

    #[staticmethod]
    fn with_multiple(selections: Vec<(&str, PartitionId)>) -> Self {
        let vals = selections
            .into_iter()
            .map(|(topic, partitions)| (topic.to_owned(), partitions))
            .collect();
        Self {
            inner: NativePartitionSelectionStrategy::Multiple(vals),
        }
    }
}

#[pyclass]
pub struct FluvioConfig {
    inner: NativeFluvioConfig,
}

#[pymethods]
impl FluvioConfig {
    #[staticmethod]
    /// Load config file from default config dir
    pub fn load() -> Result<FluvioConfig, FluvioError> {
        let inner = NativeFluvioConfig::load()?;

        Ok(FluvioConfig { inner })
    }

    #[staticmethod]
    /// Create without tls
    pub fn new(addr: &str) -> FluvioConfig {
        let inner = NativeFluvioConfig::new(addr);

        FluvioConfig { inner }
    }

    pub fn set_endpoint(&mut self, endpoint: &str) {
        self.inner.endpoint = endpoint.to_owned();
    }

    pub fn set_use_spu_local_address(&mut self, val: bool) {
        self.inner.use_spu_local_address = val;
    }

    pub fn disable_tls(&mut self) {
        self.inner.tls = TlsPolicy::Disabled;
    }

    pub fn set_anonymous_tls(&mut self) {
        self.inner.tls = TlsPolicy::Anonymous;
    }

    pub fn set_inline_tls(&mut self, domain: &str, key: &str, cert: &str, ca_cert: &str) {
        self.inner.tls = TlsPolicy::Verified(TlsConfig::Inline(TlsCerts {
            domain: domain.to_owned(),
            key: key.to_owned(),
            cert: cert.to_owned(),
            ca_cert: ca_cert.to_owned(),
        }));
    }

    pub fn set_tls_file_paths(
        &mut self,
        domain: &str,
        key_path: &str,
        cert_path: &str,
        ca_cert_path: &str,
    ) {
        self.inner.tls = TlsPolicy::Verified(TlsConfig::Files(TlsPaths {
            domain: domain.to_owned(),
            key: key_path.into(),
            cert: cert_path.into(),
            ca_cert: ca_cert_path.into(),
        }));
    }

    pub fn set_client_id(&mut self, id: &str) {
        self.inner.client_id = Some(id.to_owned());
    }

    pub fn unset_client_id(&mut self) {
        self.inner.client_id = None;
    }
}

#[pyclass]
struct _NativeConsumerConfig(NativeConsumerConfig);

#[pyclass]
pub struct ConsumerConfig {
    pub builder: ConsumerConfigBuilder,
    pub smartmodules: Vec<SmartModuleInvocation>,
}

#[pymethods]
impl ConsumerConfig {
    #[new]
    fn new() -> Self {
        Self {
            builder: NativeConsumerConfig::builder(),
            smartmodules: Vec::new(),
        }
    }

    fn max_bytes(&mut self, max_bytes: i32) {
        self.builder.max_bytes(max_bytes);
    }

    #[pyo3(signature = (name, path, kind, param_keys, param_values, aggregate_accumulator, context=None, join_param=None, join_topic=None, join_derived_stream=None))]
    fn smartmodule(
        &mut self,
        name: Option<String>,
        path: Option<String>,
        kind: Option<SmartModuleKind>,
        param_keys: Vec<String>,
        param_values: Vec<String>,

        aggregate_accumulator: Option<Vec<u8>>,
        context: Option<SmartModuleContextData>,
        join_param: Option<String>,
        join_topic: Option<String>,
        join_derived_stream: Option<String>,
    ) -> Result<(), FluvioError> {
        let kind: NativeSmartModuleKind = if let Some(kind) = kind {
            match kind {
                SmartModuleKind::Filter => NativeSmartModuleKind::Filter,
                SmartModuleKind::Map => NativeSmartModuleKind::Map,
                SmartModuleKind::ArrayMap => NativeSmartModuleKind::ArrayMap,
                SmartModuleKind::FilterMap => NativeSmartModuleKind::FilterMap,
                SmartModuleKind::Aggregate => NativeSmartModuleKind::Aggregate {
                    accumulator: aggregate_accumulator.unwrap_or_default(),
                },
                SmartModuleKind::Join => {
                    NativeSmartModuleKind::Join(join_param.unwrap_or_default())
                }
                SmartModuleKind::JoinStream => NativeSmartModuleKind::JoinStream {
                    topic: join_topic.unwrap_or_default(),
                    derivedstream: join_derived_stream.unwrap_or_default(),
                },
                _ => NativeSmartModuleKind::default(), // default is Filter.
            }
        } else {
            match context {
                Some(SmartModuleContextData::Aggregate) => {
                    NativeSmartModuleKind::Generic(NativeSmartModuleContextData::Aggregate {
                        accumulator: aggregate_accumulator.unwrap_or_default(),
                    })
                }
                Some(SmartModuleContextData::Join) => NativeSmartModuleKind::Generic(
                    NativeSmartModuleContextData::Join(join_param.unwrap_or_default()),
                ),
                Some(SmartModuleContextData::JoinStream) => {
                    NativeSmartModuleKind::Generic(NativeSmartModuleContextData::JoinStream {
                        topic: join_topic.unwrap_or_default(),
                        derivedstream: join_derived_stream.unwrap_or_default(),
                    })
                }
                None => {
                    if let Some(accumulator) = aggregate_accumulator {
                        NativeSmartModuleKind::Generic(NativeSmartModuleContextData::Aggregate {
                            accumulator,
                        })
                    } else {
                        NativeSmartModuleKind::Generic(NativeSmartModuleContextData::default())
                    }
                }
            }
        };
        use std::collections::BTreeMap;
        let params: Vec<(String, String)> = param_keys
            .into_iter()
            .zip(param_values.into_iter())
            .collect();
        let params: BTreeMap<String, String> = BTreeMap::from_iter(params);
        let params: SmartModuleExtraParams = SmartModuleExtraParams::from(params);

        if let Some(name) = name {
            self.smartmodules.push(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::Predefined(name),
                kind: kind.clone(),
                params: params.clone(),
            });
        }
        if let Some(path) = path {
            let wasm_module_buffer = std::fs::read(path)?;
            self.smartmodules.push(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::adhoc_from_bytes(wasm_module_buffer.as_slice())?,
                kind,
                params,
            });
        }
        Ok(())
    }

    fn build(&mut self) -> Result<_NativeConsumerConfig, FluvioError> {
        let config = self.builder.smartmodule(self.smartmodules.clone());
        Ok(_NativeConsumerConfig(config.build()?))
    }
}

#[derive(Clone)]
#[pyclass]
pub enum SmartModuleKind {
    Filter,
    Map,
    ArrayMap,
    FilterMap,
    Join,
    JoinStream,
    Aggregate,
    Generic,
}

#[derive(Debug, Clone)]
#[pyclass]
pub enum SmartModuleContextData {
    Aggregate,
    Join,
    JoinStream,
}

#[pyclass]
struct Offset(NativeOffset);

#[pymethods]
impl Offset {
    #[staticmethod]
    fn absolute(index: i64) -> Result<Offset, FluvioError> {
        Ok(Offset(NativeOffset::absolute(index)?))
    }

    #[staticmethod]
    fn beginning() -> Offset {
        Offset(NativeOffset::beginning())
    }

    #[staticmethod]
    fn from_beginning(offset: u32) -> Offset {
        Offset(NativeOffset::from_beginning(offset))
    }

    #[staticmethod]
    fn end() -> Offset {
        Offset(NativeOffset::end())
    }

    #[staticmethod]
    fn from_end(offset: u32) -> Offset {
        Offset(NativeOffset::from_end(offset))
    }
}

#[pyclass]
struct PartitionConsumer(NativePartitionConsumer);

impl Clone for PartitionConsumer {
    fn clone(&self) -> Self {
        PartitionConsumer(self.0.clone())
    }
}

#[pymethods]
impl PartitionConsumer {
    fn stream(&self, offset: &Offset) -> Result<PartitionConsumerStream, FluvioError> {
        Ok(PartitionConsumerStream {
            inner: Box::pin(run_block_on(self.0.stream(offset.0.clone()))?),
        })
    }
    fn async_stream<'b>(&'b self, offset: &Offset, py: Python<'b>) -> PyResult<&PyAny> {
        let sl = self.clone();
        let offset = offset.0.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let stream =
                sl.0.stream(offset)
                    .await
                    .map_err(|err| FluvioError::AnyhowError(err))?;
            Ok(Python::with_gil(|py| {
                Py::new(
                    py,
                    PartitionConsumerStream {
                        inner: Box::pin(stream),
                    },
                )
                .unwrap()
            }))
        })
    }
    fn stream_with_config(
        &self,
        offset: &Offset,
        config: &mut ConsumerConfig,
    ) -> Result<PartitionConsumerStream, FluvioError> {
        // let config = config.build()?;
        let config: NativeConsumerConfig = config.build()?.0;

        Ok(
            run_block_on(self.0.stream_with_config(offset.0.clone(), config)).map(|stream| {
                PartitionConsumerStream {
                    inner: Box::pin(stream),
                }
            })?,
        )
    }
    fn async_stream_with_config<'b>(
        &'b self,
        offset: &Offset,
        config: &mut ConsumerConfig,
        py: Python<'b>,
    ) -> PyResult<&PyAny> {
        let sl = self.clone();
        let offset = offset.0.clone();
        let config: NativeConsumerConfig = config.build()?.0;
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let stream =
                sl.0.stream_with_config(offset, config)
                    .await
                    .map_err(|err| FluvioError::AnyhowError(err))?;
            Ok(Python::with_gil(|py| {
                Py::new(
                    py,
                    PartitionConsumerStream {
                        inner: Box::pin(stream),
                    },
                )
                .unwrap()
            }))
        })
    }
}

#[pyclass]
struct MultiplePartitionConsumer(NativeMultiplePartitionConsumer);

impl Clone for MultiplePartitionConsumer {
    fn clone(&self) -> Self {
        MultiplePartitionConsumer(self.0.clone())
    }
}

#[pymethods]
impl MultiplePartitionConsumer {
    fn stream(&self, offset: &Offset) -> Result<PartitionConsumerStream, FluvioError> {
        Ok(PartitionConsumerStream {
            inner: Box::pin(run_block_on(self.0.stream(offset.0.clone()))?),
        })
    }
    fn async_stream<'b>(&'b self, offset: &Offset, py: Python<'b>) -> PyResult<&PyAny> {
        let sl = self.clone();
        let offset = offset.0.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let stream =
                sl.0.stream(offset)
                    .await
                    .map_err(|err| FluvioError::AnyhowError(err))?;
            Ok(Python::with_gil(|py| {
                Py::new(
                    py,
                    PartitionConsumerStream {
                        inner: Box::pin(stream),
                    },
                )
                .unwrap()
            }))
        })
    }
    fn stream_with_config(
        &self,
        offset: &Offset,
        config: &mut ConsumerConfig,
    ) -> Result<PartitionConsumerStream, FluvioError> {
        // let config = config.build()?;
        let config: NativeConsumerConfig = config.build()?.0;

        Ok(
            run_block_on(self.0.stream_with_config(offset.0.clone(), config)).map(|stream| {
                PartitionConsumerStream {
                    inner: Box::pin(stream),
                }
            })?,
        )
    }
    fn async_stream_with_config<'b>(
        &'b self,
        offset: &Offset,
        config: &mut ConsumerConfig,
        py: Python<'b>,
    ) -> PyResult<&PyAny> {
        let sl = self.clone();
        let offset = offset.0.clone();
        let config: NativeConsumerConfig = config.build()?.0;
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let stream =
                sl.0.stream_with_config(offset, config)
                    .await
                    .map_err(|err| FluvioError::AnyhowError(err))?;
            Ok(Python::with_gil(|py| {
                Py::new(
                    py,
                    PartitionConsumerStream {
                        inner: Box::pin(stream),
                    },
                )
                .unwrap()
            }))
        })
    }
}

type PartitionConsumerIteratorInner =
    Pin<Box<dyn Stream<Item = Result<NativeRecord, ErrorCode>> + Send>>;

#[pyclass]
pub struct PartitionConsumerStream {
    pub inner: PartitionConsumerIteratorInner,
}

#[pymethods]
impl PartitionConsumerStream {
    fn next(&mut self) -> Result<Option<Record>, PyErr> {
        Ok(Some(Record(
            run_block_on(self.inner.next())
                .unwrap()
                .map_err(|err| PyException::new_err(err.to_string()))?,
        )))
    }
}

#[derive(Clone)]
#[pyclass]
pub struct ProducerBatchRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[pymethods]
impl ProducerBatchRecord {
    #[new]
    fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

#[derive(Clone)]
#[pyclass]
struct TopicProducer(NativeTopicProducer);

#[pymethods]
impl TopicProducer {
    fn send(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), FluvioError> {
        Ok(run_block_on(self.0.send(key, value)).map(|_| ())?)
    }
    fn async_send<'b>(&'b self, key: Vec<u8>, value: Vec<u8>, py: Python<'b>) -> PyResult<&PyAny> {
        let sl = self.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            sl.0.send(key, value)
                .await
                .map(|_| ())
                .map_err(|err| FluvioError::AnyhowError(err))?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
    fn send_all(&self, records: Vec<ProducerBatchRecord>) -> Result<(), FluvioError> {
        Ok(run_block_on(
            self.0
                .send_all(records.iter().map(|record| -> (Vec<u8>, Vec<u8>) {
                    (record.key.clone(), record.value.clone())
                })),
        )
        .map(|_| ())?)
    }
    fn async_send_all<'b>(
        &'b self,
        records: Vec<ProducerBatchRecord>,
        py: Python<'b>,
    ) -> PyResult<&PyAny> {
        let sl = self.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            sl.0.send_all(
                records
                    .into_iter()
                    .map(|record| -> (Vec<u8>, Vec<u8>) { (record.key, record.value) }),
            )
            .await
            .map(|_| ())
            .map_err(|err| FluvioError::AnyhowError(err))?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
    fn flush(&self) -> Result<(), FluvioError> {
        Ok(run_block_on(self.0.flush())?)
    }
    fn async_flush<'b>(&'b self, py: Python<'b>) -> PyResult<&PyAny> {
        let sl = self.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            sl.0.flush()
                .map_err(|err| FluvioError::AnyhowError(err))
                .await?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}

#[pyclass]
pub struct Record(NativeRecord);

#[pymethods]
impl Record {
    fn value_string(&self) -> Result<String, PyErr> {
        String::from_utf8(self.0.value().to_vec()).map_err(utf8_to_py_err)
    }

    fn key_string(&self) -> Result<String, PyErr> {
        let key = self.0.key().unwrap_or(b"");
        String::from_utf8(key.to_vec()).map_err(utf8_to_py_err)
    }

    fn offset(&self) -> i64 {
        self.0.offset
    }

    fn value(&self) -> Vec<u8> {
        self.0.value().to_vec()
    }

    fn key(&self) -> Result<Vec<u8>, PyErr> {
        Ok(self.0.key().unwrap_or(b"No key").to_vec())
    }

    fn timestamp(&self) -> i64 {
        self.0.timestamp()
    }
}

#[pyclass]
struct Cloud {
    DEFAULT_PROFILE_NAME: &'static str, // = "cloud"
}

#[pymethods]
impl Cloud {
    #[staticmethod]
    pub fn login_with_username(
        remote: String,
        profile: Option<String>,
        email: Option<String>,
        password: Option<String>,
    ) -> Result<(), CloudLoginError> {
        run_block_on(async {
            let mut client = CloudClient::with_default_path()?;
            let email = match email {
                Some(email) => email.clone(),
                None => {
                    print!("Infinyon Cloud email: ");
                    io::stdout().flush()?;
                    let mut email = String::new();
                    io::stdin().read_line(&mut email)?;
                    email
                }
            };
            let email = email.trim();
            let password = match password {
                Some(pw) => pw.clone(),
                None => rpassword::prompt_password("Password: ")?,
            };
            client.authenticate(email, &password, &remote).await?;

            let cluster = match client.download_profile().await {
                Ok(cluster) => cluster,
                Err(CloudLoginError::ClusterDoesNotExist(_))
                | Err(CloudLoginError::ProfileNotAvailable) => {
                    println!("Warning: You don't have any clusters, please create cluster if you want to perform fluvio functions");
                    return Ok(());
                }
                Err(err) => {
                    return Err(err);
                }
            };
            println!("Fluvio cluster found, switching to profile");

            save_cluster(cluster, remote, profile)?;
            Ok(())
        })
    }
}

fn save_cluster(
    cluster: NativeFluvioConfig,
    remote: String,
    profile: Option<String>,
) -> Result<(), CloudLoginError> {
    let mut config_file = ConfigFile::load_default_or_new()?;
    let config = config_file.mut_config();
    let profile_name = if let Some(profile) = profile {
        profile
    } else {
        profile_from_remote(remote).unwrap_or_else(|| "cloud".to_string())
    };

    let profile = Profile::new(profile_name.clone());
    config.add_cluster(cluster, profile_name.clone());
    config.add_profile(profile, profile_name.clone());
    config.set_current_profile(&profile_name);
    config_file.save()?;
    info!(%profile_name, "Successfully saved profile");
    Ok(())
}

fn profile_from_remote(remote: String) -> Option<String> {
    let url = url::Url::parse(remote.as_str()).ok()?;
    let host = url.host()?;
    match host {
        Host::Ipv4(ip4) => Some(format!("{}", ip4)),
        Host::Ipv6(ip6) => Some(format!("{}", ip6)),
        Host::Domain(domain) => Some(domain.to_owned().replace('.', "-")),
    }
}

pub struct CloudAuth {
    pub auth0_config: Option<cloud::Auth0Config>,
    pub device_code: Option<cloud::DeviceCodeResponse>,
    pub client: CloudClient,
    remote: String,
    profile: Option<String>,
}
impl CloudAuth {
    pub fn new(remote: String) -> Result<CloudAuth, CloudLoginError> {
        run_block_on(async {
            let client = CloudClient::with_default_path()?;
            Ok(CloudAuth {
                client,
                remote,
                auth0_config: None,
                device_code: None,
                profile: None,
            })
        })
    }

    pub fn get_auth0_url(&mut self) -> Result<(String, String), CloudLoginError> {
        run_block_on(async {
            let (auth0_config, device_code) = self
                .client
                .get_auth0_and_device_code(self.remote.as_str())
                .await?;
            let (complete_url, user_code) = (
                device_code.verification_uri_complete.clone(),
                device_code.user_code.clone(),
            );
            self.auth0_config = Some(auth0_config);
            self.device_code = Some(device_code);
            Ok((complete_url, user_code))
        })
    }

    pub fn authenticate_with_auth0(&mut self) -> Result<(), CloudLoginError> {
        run_block_on(async {
            let auth0_config = self
                .auth0_config
                .as_ref()
                .ok_or(CloudLoginError::Auth0ConfigNotFound)?;
            let device_code = self
                .device_code
                .as_ref()
                .ok_or(CloudLoginError::DeviceCodeNotFound)?;
            self.client
                .authenticate_with_auth0(self.remote.as_str(), auth0_config, device_code)
                .await?;
            let cluster = match self.client.download_profile().await {
                Ok(cluster) => cluster,
                Err(CloudLoginError::ClusterDoesNotExist(_))
                | Err(CloudLoginError::ProfileNotAvailable) => {
                    println!("Warning: You don't have any clusters, please create cluster if you want to perform fluvio functions");
                    return Ok(());
                }
                Err(err) => {
                    return Err(err);
                }
            };
            println!("Fluvio cluster found, switching to profile");

            save_cluster(cluster, self.remote.clone(), self.profile.clone())?;
            Ok(())
        })
    }
}
