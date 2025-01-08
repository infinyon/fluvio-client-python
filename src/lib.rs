#![allow(non_snake_case, unused)]

use fluvio::config::{ConfigFile, Profile, TlsCerts, TlsConfig, TlsPaths, TlsPolicy};
use fluvio::consumer::{
    ConsumerConfig as NativeConsumerConfig, ConsumerConfigBuilder,
    ConsumerOffset as NativeConsumerOffset, ConsumerStream,
    SmartModuleContextData as NativeSmartModuleContextData, SmartModuleExtraParams,
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind as NativeSmartModuleKind,
};
use fluvio::dataplane::link::ErrorCode;
use fluvio::{
    consumer::Record as NativeRecord, Fluvio as NativeFluvio, FluvioAdmin as NativeFluvioAdmin,
    ProduceOutput as NativeProduceOutput, RecordMetadata as NativeRecordMetadata,
};
use fluvio::{
    FluvioConfig as NativeFluvioConfig,
    MultiplePartitionConsumer as NativeMultiplePartitionConsumer, Offset as NativeOffset,
    PartitionConsumer as NativePartitionConsumer,
    PartitionSelectionStrategy as NativePartitionSelectionStrategy,
    TopicProducerPool as NativeTopicProducer,
};
use fluvio_controlplane_metadata::message::{Message as NativeMessage, MsgType as NativeMsgType};
use fluvio_controlplane_metadata::partition::PartitionSpec as NativePartitionSpec;
use fluvio_controlplane_metadata::smartmodule::{
    SmartModuleWasm as NativeSmartModuleWasm, SmartModuleWasmFormat as NativeSmartModuleWasmFormat,
};
use fluvio_controlplane_metadata::topic::{
    PartitionMap as NativePartitionMap, TopicSpec as NativeTopicSpec,
};
use fluvio_future::io::FutureExt;
use fluvio_future::timer::sleep;
use fluvio_future::{
    io::{Stream, StreamExt},
    task::run_block_on,
};
use fluvio_sc_schema::objects::{
    CommonCreateRequest as NativeCommonCreateRequest, Metadata as NativeMetadata,
    MetadataUpdate as NativeMetadataUpdate, WatchResponse as NativeWatchResponse,
};
use fluvio_sc_schema::smartmodule::SmartModuleSpec as NativeSmartModuleSpec;
use fluvio_sc_schema::topic::{
    CleanupPolicy, CompressionAlgorithm, Deduplication, HomeMirrorConfig, MirrorConfig,
    PartitionMaps as NativePartitionMaps, ReplicaSpec, SegmentBasedPolicy, TopicStorageConfig,
};
use fluvio_types::{compression, PartitionId};
use fluvio_types::{
    IgnoreRackAssignment as NativeIgnoreRackAssignment, PartitionCount as NativePartitionCount,
    PartitionId as NativePartitionId, ReplicationFactor as NativeReplicationFactor,
    SpuId as NativeSpuId,
};
use futures::future::BoxFuture;
use futures::pin_mut;
use futures::TryFutureExt;
use std::future::Future;
use std::io::{self, Error as IoError, Write};
use std::pin::Pin;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tracing::info;
use url::Host;
mod cloud;

mod consumer;
mod error;
mod produce_output;

pub use produce_output::ProduceOutput;

use cloud::{CloudClient, CloudLoginError};
use error::FluvioError;

use consumer::{ConsumerConfigExt, ConsumerConfigExtBuilder, OffsetManagementStrategy};

use pyo3::exceptions::{PyException, PyValueError};
use pyo3::types::PyList;
use pyo3::{prelude::*, IntoPyObjectExt};

pyo3::create_exception!(mymodule, PyFluvioError, PyException);

#[pymodule]
fn _fluvio_python(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Fluvio>()?;
    m.add_class::<FluvioConfig>()?;
    m.add_class::<ConsumerConfig>()?;
    m.add_class::<ConsumerConfigExt>()?;
    m.add_class::<ConsumerConfigExtBuilder>()?;
    m.add_class::<OffsetManagementStrategy>()?;
    m.add_class::<PartitionConsumer>()?;
    m.add_class::<PartitionConsumerStream>()?;
    m.add_class::<AsyncPartitionConsumerStream>()?;
    m.add_class::<TopicProducer>()?;
    m.add_class::<ProduceOutput>()?;
    m.add_class::<RecordMetadata>()?;
    m.add_class::<ProducerBatchRecord>()?;
    m.add_class::<SmartModuleKind>()?;
    m.add_class::<Record>()?;
    m.add_class::<Offset>()?;
    m.add_class::<Cloud>()?;
    m.add_class::<ConsumerOffset>()?;
    m.add_class::<MultiplePartitionConsumer>()?;
    m.add_class::<PartitionSelectionStrategy>()?;
    m.add_class::<FluvioAdmin>()?;
    m.add_class::<TopicSpec>()?;
    m.add_class::<PartitionMap>()?;
    m.add_class::<CommonCreateRequest>()?;
    m.add_class::<MetadataTopicSpec>()?;
    m.add_class::<WatchTopicStream>()?;
    m.add_class::<MetaUpdateTopicSpec>()?;
    m.add_class::<MessageMetadataTopicSpec>()?;
    m.add_class::<SmartModuleSpec>()?;
    m.add_class::<MetadataSmartModuleSpec>()?;
    m.add_class::<WatchSmartModuleStream>()?;
    m.add_class::<MessageMetadataSmartModuleSpec>()?;
    m.add_class::<MetaUpdateSmartModuleSpec>()?;
    m.add_class::<MetadataPartitionSpec>()?;
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
    fn connect(py: Python) -> PyResult<Fluvio> {
        Ok(Fluvio(py.allow_threads(move || {
            run_block_on(NativeFluvio::connect()).map_err(error_to_py_err)
        })?))
    }

    #[staticmethod]
    fn connect_with_config(config: &FluvioConfig, py: Python) -> PyResult<Fluvio> {
        Ok(Fluvio(py.allow_threads(move || {
            run_block_on(NativeFluvio::connect_with_config(&config.inner)).map_err(error_to_py_err)
        })?))
    }

    fn consumer_with_config(
        &self,
        config: &ConsumerConfigExt,
        py: Python,
    ) -> PyResult<PartitionConsumerStream> {
        let inner = py.allow_threads(move || {
            run_block_on(self.0.consumer_with_config(config.inner.to_owned()))
                .map_err(error_to_py_err)
        })?;
        Ok(PartitionConsumerStream {
            inner: Box::pin(inner),
        })
    }

    fn partition_consumer(
        &self,
        topic: String,
        partition: u32,
        py: Python,
    ) -> PyResult<PartitionConsumer> {
        #[allow(deprecated)]
        Ok(PartitionConsumer(py.allow_threads(move || {
            run_block_on(self.0.partition_consumer(topic, partition)).map_err(error_to_py_err)
        })?))
    }

    fn multi_partition_consumer(
        &self,
        strategy: PartitionSelectionStrategy,
        py: Python,
    ) -> PyResult<MultiplePartitionConsumer> {
        #[allow(deprecated)]
        Ok(MultiplePartitionConsumer(py.allow_threads(move || {
            run_block_on(self.0.consumer(strategy.into_inner())).map_err(error_to_py_err)
        })?))
    }

    fn topic_producer(&self, topic: String, py: Python) -> PyResult<TopicProducer> {
        let native_prod = py.allow_threads(move || {
            run_block_on(self.0.topic_producer(topic)).map_err(error_to_py_err)
        })?;
        Ok(TopicProducer(Arc::new(native_prod)))
    }

    fn consumer_offsets(&self, py: Python) -> PyResult<Vec<ConsumerOffset>> {
        py.allow_threads(move || {
            run_block_on(self.0.consumer_offsets())
                .map(|offsets| offsets.into_iter().map(ConsumerOffset::from).collect())
                .map_err(error_to_py_err)
        })
    }

    fn delete_consumer_offset(
        &self,
        consumer_id: String,
        topic: String,
        partition: u32,
    ) -> PyResult<()> {
        run_block_on(
            self.0
                .delete_consumer_offset(consumer_id, (topic, partition)),
        )
        .map_err(error_to_py_err)
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct ConsumerOffset {
    #[pyo3(get)]
    consumer_id: String,
    #[pyo3(get)]
    topic: String,
    #[pyo3(get)]
    partition: u32,
    #[pyo3(get)]
    offset: i64,
    #[pyo3(get)]
    modified_time: u64,
}

impl From<NativeConsumerOffset> for ConsumerOffset {
    fn from(offset: NativeConsumerOffset) -> Self {
        ConsumerOffset {
            consumer_id: offset.consumer_id,
            topic: offset.topic,
            partition: offset.partition,
            offset: offset.offset,
            modified_time: offset.modified_time,
        }
    }
}

#[derive(Clone)]
#[pyclass]
pub struct PartitionSelectionStrategy {
    inner: NativePartitionSelectionStrategy,
}

impl PartitionSelectionStrategy {
    fn into_inner(self) -> NativePartitionSelectionStrategy {
        self.inner
    }
}

#[pymethods]
impl PartitionSelectionStrategy {
    #[staticmethod]
    fn with_all(topic: &str) -> Self {
        Self {
            inner: NativePartitionSelectionStrategy::All(topic.to_owned()),
        }
    }

    #[staticmethod]
    fn with_multiple(selections: Vec<(String, PartitionId)>) -> Self {
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
        endpoint.clone_into(&mut self.inner.endpoint)
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

    fn disable_continuous(&mut self, setting: bool) {
        self.builder.disable_continuous(setting);
    }

    #[allow(clippy::too_many_arguments)]
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
        let params: Vec<(String, String)> = param_keys.into_iter().zip(param_values).collect();
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

#[derive(Debug, Clone, PartialEq)]
#[pyclass(eq, eq_int)]
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

#[derive(Debug, Clone, PartialEq)]
#[pyclass(eq, eq_int)]
pub enum SmartModuleContextData {
    Aggregate,
    Join,
    JoinStream,
}

/// Describes the location of a record stored in a Fluvio partition.
///
/// A topic is composed of one or more partitions.
#[pyclass]
struct Offset(NativeOffset);

#[pymethods]
impl Offset {
    /// Specifies an absolute offset with the given index within the partition
    #[staticmethod]
    fn absolute(index: i64) -> Result<Offset, FluvioError> {
        Ok(Offset(NativeOffset::absolute(index)?))
    }

    /// Specifies an offset starting at the beginning of the partition
    #[staticmethod]
    fn beginning() -> Offset {
        Offset(NativeOffset::beginning())
    }

    /// Specifies an offset relative to the beginning of the partition
    #[staticmethod]
    fn from_beginning(offset: u32) -> Offset {
        Offset(NativeOffset::from_beginning(offset))
    }

    /// Specifies an offset relative to the beginning of the partition
    #[staticmethod]
    fn end() -> Offset {
        Offset(NativeOffset::end())
    }

    /// Specifies an offset relative to the beginning of the partition
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
    fn stream(&self, offset: &Offset) -> Result<PartitionConsumerIterator, FluvioError> {
        #[allow(deprecated)]
        Ok(PartitionConsumerIterator {
            inner: Box::pin(run_block_on(self.0.stream(offset.0.clone()))?),
        })
    }
    fn async_stream<'b>(&'b self, offset: &Offset, py: Python<'b>) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        let offset = offset.0.clone();
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            #[allow(deprecated)]
            let stream =
                sl.0.stream(offset)
                    .await
                    .map_err(FluvioError::AnyhowError)?;
            Ok(Python::with_gil(|py| {
                Py::new(py, AsyncPartitionConsumerStream::new(Box::new(stream))).unwrap()
            }))
        })
    }
    fn stream_with_config(
        &self,
        offset: &Offset,
        config: &mut ConsumerConfig,
        py: Python,
    ) -> Result<PartitionConsumerIterator, FluvioError> {
        let config: NativeConsumerConfig = config.build()?.0;
        Ok(py.allow_threads(move || {
            #[allow(deprecated)]
            run_block_on(self.0.stream_with_config(offset.0.clone(), config)).map(|stream| {
                PartitionConsumerIterator {
                    inner: Box::pin(stream),
                }
            })
        })?)
    }
    fn async_stream_with_config<'b>(
        &'b self,
        offset: &Offset,
        config: &mut ConsumerConfig,
        py: Python<'b>,
    ) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        let offset = offset.0.clone();
        let config: NativeConsumerConfig = config.build()?.0;
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            #[allow(deprecated)]
            let stream =
                sl.0.stream_with_config(offset, config)
                    .await
                    .map_err(FluvioError::AnyhowError)?;
            Ok(Python::with_gil(|py| {
                Py::new(py, AsyncPartitionConsumerStream::new(stream)).unwrap()
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
    fn stream(
        &self,
        offset: &Offset,
        py: Python,
    ) -> Result<PartitionConsumerIterator, FluvioError> {
        #[allow(deprecated)]
        Ok(PartitionConsumerIterator {
            inner: Box::pin(
                py.allow_threads(move || run_block_on(self.0.stream(offset.0.clone())))?,
            ),
        })
    }
    fn async_stream<'b>(&'b self, offset: &Offset, py: Python<'b>) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        let offset = offset.0.clone();
        #[allow(deprecated)]
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            let stream =
                sl.0.stream(offset)
                    .await
                    .map_err(FluvioError::AnyhowError)?;
            Ok(Python::with_gil(|py| {
                Py::new(py, AsyncPartitionConsumerStream::new(stream)).unwrap()
            }))
        })
    }
    fn stream_with_config(
        &self,
        offset: &Offset,
        config: &mut ConsumerConfig,
        py: Python,
    ) -> Result<PartitionConsumerIterator, FluvioError> {
        let config: NativeConsumerConfig = config.build()?.0;

        Ok(py.allow_threads(move || {
            #[allow(deprecated)]
            run_block_on(self.0.stream_with_config(offset.0.clone(), config)).map(|stream| {
                PartitionConsumerIterator {
                    inner: Box::pin(stream),
                }
            })
        })?)
    }
    fn async_stream_with_config<'b>(
        &'b self,
        offset: &Offset,
        config: &mut ConsumerConfig,
        py: Python<'b>,
    ) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        let offset = offset.0.clone();
        let config: NativeConsumerConfig = config.build()?.0;
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            #[allow(deprecated)]
            let stream =
                sl.0.stream_with_config(offset, config)
                    .await
                    .map_err(FluvioError::AnyhowError)?;
            Ok(Python::with_gil(|py| {
                Py::new(py, AsyncPartitionConsumerStream::new(stream)).unwrap()
            }))
        })
    }
}

type PartitionConsumerIteratorInner =
    Pin<Box<dyn Stream<Item = Result<NativeRecord, ErrorCode>> + Send>>;

#[pyclass(unsendable)]
pub struct PartitionConsumerIterator {
    pub inner: PartitionConsumerIteratorInner,
}

#[pymethods]
impl PartitionConsumerIterator {
    fn next(&mut self, py: Python) -> Result<Option<Record>, PyErr> {
        match run_future_checking_signals(self.inner.next(), py) {
            Ok(Some(Ok(rec))) => Ok(Some(Record(rec))),
            Ok(Some(Err(err))) => match err {
                ErrorCode::OffsetOutOfRange => Ok(None),
                err => Err(PyException::new_err(err.to_string())),
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err), // Python exception
        }
    }
}

type PartitionConsumerStreamInner =
    Pin<Box<dyn ConsumerStream<Item = Result<NativeRecord, ErrorCode>> + Send>>;

#[pyclass(unsendable)]
pub struct PartitionConsumerStream {
    pub inner: PartitionConsumerStreamInner,
}

#[pymethods]
impl PartitionConsumerStream {
    fn next(&mut self, py: Python) -> Result<Option<Record>, PyErr> {
        match run_future_checking_signals(self.inner.next(), py) {
            Ok(Some(Ok(rec))) => Ok(Some(Record(rec))),
            Ok(Some(Err(err))) => match err {
                ErrorCode::OffsetOutOfRange => Ok(None),
                err => Err(PyException::new_err(err.to_string())),
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err), // Python exception
        }
    }

    fn offset_commit(&mut self, py: Python) -> Result<(), PyErr> {
        self.inner
            .offset_commit()
            .map_err(|err| PyException::new_err(err.to_string()))
    }

    fn offset_flush(&mut self, py: Python) -> Result<(), PyErr> {
        py.allow_threads(move || run_block_on(self.inner.offset_flush()))
            .map_err(|err| PyException::new_err(err.to_string()))
    }
}

type AsyncPartitionConsumerIteratorInner =
    Arc<async_lock::Mutex<Pin<Box<dyn Stream<Item = Result<NativeRecord, ErrorCode>> + Send>>>>;

#[derive(Clone)]
#[pyclass]
pub struct AsyncPartitionConsumerStream {
    pub inner: AsyncPartitionConsumerIteratorInner,
}

impl AsyncPartitionConsumerStream {
    pub fn new(s: impl Stream<Item = Result<NativeRecord, ErrorCode>> + Send + 'static) -> Self {
        Self {
            inner: Arc::new(async_lock::Mutex::new(Box::pin(s))),
        }
    }
}

#[pymethods]
impl AsyncPartitionConsumerStream {
    pub fn async_next<'b>(&mut self, py: Python<'b>) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            let record = sl
                .inner
                .lock()
                .await
                .next()
                .await
                .unwrap()
                .map_err(|err| PyException::new_err(err.to_string()))?;
            Ok(Python::with_gil(|py| Py::new(py, Record(record)).unwrap()))
        })
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

#[derive(Debug)]
#[pyclass]
pub struct RecordMetadata {
    pub inner: NativeRecordMetadata,
}

#[pymethods]
impl RecordMetadata {
    fn offset(&self) -> i64 {
        self.inner.offset()
    }

    fn partition_id(&self) -> PartitionId {
        self.inner.partition_id()
    }
}

#[derive(Clone)]
#[pyclass]
struct TopicProducer(Arc<NativeTopicProducer>);

#[pymethods]
impl TopicProducer {
    fn send(&self, key: Vec<u8>, value: Vec<u8>, py: Python) -> Result<ProduceOutput, FluvioError> {
        Ok(py.allow_threads(move || {
            run_block_on(self.0.send(key, value)).map(|output| ProduceOutput {
                inner: Some(output),
            })
        })?)
    }
    fn async_send<'b>(
        &'b self,
        key: Vec<u8>,
        value: Vec<u8>,
        py: Python<'b>,
    ) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            let produce_output =
                sl.0.send(key, value)
                    .await
                    .map(|output| ProduceOutput {
                        inner: Some(output),
                    })
                    .map_err(FluvioError::AnyhowError)?;
            Ok(Python::with_gil(|py| produce_output.into_py(py)))
        })
    }
    fn send_all(
        &self,
        records: Vec<ProducerBatchRecord>,
        py: Python,
    ) -> Result<Vec<ProduceOutput>, FluvioError> {
        Ok(py.allow_threads(move || {
            run_block_on(
                self.0
                    .send_all(records.iter().map(|record| -> (Vec<u8>, Vec<u8>) {
                        (record.key.clone(), record.value.clone())
                    })),
            )
            .map(|outputs| {
                outputs
                    .into_iter()
                    .map(|output| ProduceOutput {
                        inner: Some(output),
                    })
                    .collect()
            })
        })?)
    }
    fn async_send_all<'b>(
        &'b self,
        records: Vec<ProducerBatchRecord>,
        py: Python<'b>,
    ) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            let produce_outputs =
                sl.0.send_all(
                    records
                        .into_iter()
                        .map(|record| -> (Vec<u8>, Vec<u8>) { (record.key, record.value) }),
                )
                .await
                .map_err(FluvioError::AnyhowError)?;

            Ok(Python::with_gil(|py| {
                let lst = PyList::new(
                    py,
                    produce_outputs
                        .into_iter()
                        .map(|po| ProduceOutput { inner: Some(po) }),
                );

                match lst {
                    Ok(lst) => lst.into(),
                    Err(_) => py.None(),
                }
            }))
        })
    }
    fn flush(&self, py: Python) -> Result<(), FluvioError> {
        Ok(py.allow_threads(move || run_block_on(self.0.flush()))?)
    }
    fn async_flush<'b>(&'b self, py: Python<'b>) -> PyResult<Bound<'b, PyAny>> {
        let sl = self.clone();
        pyo3_async_runtimes::async_std::future_into_py(py, async move {
            sl.0.flush().map_err(FluvioError::AnyhowError).await?;
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

macro_rules! create_impl {
    ($admin: ident, $name:ident, $dry_run: ident, $spec: ident) => {
        run_block_on($admin.inner.create($name, $dry_run, $spec.inner)).map_err(error_to_py_err)
    };
}

macro_rules! delete_impl {
    ($admin: ident, $name:ident, $t: ty) => {
        run_block_on($admin.inner.delete::<$t>($name)).map_err(error_to_py_err)
    };
}

macro_rules! list_impl {
    ($admin: ident, $filters:ident) => {{
        let stream = run_block_on($admin.inner.list($filters)).map_err(error_to_py_err)?;
        Ok(stream.into_iter().map(|s| s.into()).collect())
    }};
}

macro_rules! watch_impl {
    ($admin: ident, $stream_ty: ty) => {{
        let stream = run_block_on($admin.inner.watch()).map_err(error_to_py_err)?;
        Ok(<$stream_ty>::new(Box::pin(stream)))
    }};
}

#[pyclass]
struct FluvioAdmin {
    inner: NativeFluvioAdmin,
}

#[pymethods]
impl FluvioAdmin {
    #[staticmethod]
    pub fn connect() -> PyResult<FluvioAdmin> {
        Ok(FluvioAdmin {
            inner: run_block_on(NativeFluvioAdmin::connect()).map_err(error_to_py_err)?,
        })
    }

    #[staticmethod]
    pub fn connect_with_config(config: &FluvioConfig) -> PyResult<FluvioAdmin> {
        Ok(FluvioAdmin {
            inner: run_block_on(NativeFluvioAdmin::connect_with_config(&config.inner))
                .map_err(error_to_py_err)?,
        })
    }

    pub fn create_topic(&self, name: String, dry_run: bool, spec: TopicSpec) -> PyResult<()> {
        create_impl!(self, name, dry_run, spec)
    }

    pub fn create_topic_with_config(
        &self,
        rq: CommonCreateRequest,
        spec: TopicSpec,
    ) -> PyResult<()> {
        run_block_on(self.inner.create_with_config(rq.inner, spec.inner)).map_err(error_to_py_err)
    }

    pub fn delete_topic(&self, name: String) -> PyResult<()> {
        delete_impl!(self, name, NativeTopicSpec)
    }

    pub fn all_topics(&self) -> PyResult<Vec<MetadataTopicSpec>> {
        let data = run_block_on(self.inner.all()).map_err(error_to_py_err)?;
        Ok(data.into_iter().map(|s| s.into()).collect())
    }

    pub fn list_topics(&self, filters: Vec<String>) -> PyResult<Vec<MetadataTopicSpec>> {
        list_impl!(self, filters)
    }

    pub fn list_topics_with_params(
        &self,
        filters: Vec<String>,
        summary: bool,
    ) -> PyResult<Vec<MetadataTopicSpec>> {
        let data =
            run_block_on(self.inner.list_with_params(filters, summary)).map_err(error_to_py_err)?;
        Ok(data.into_iter().map(|s| s.into()).collect())
    }

    pub fn watch_topic(&self) -> PyResult<WatchTopicStream> {
        watch_impl!(self, WatchTopicStream)
    }

    pub fn create_smart_module(
        &self,
        name: String,
        dry_run: bool,
        spec: SmartModuleSpec,
    ) -> PyResult<()> {
        create_impl!(self, name, dry_run, spec)
    }

    pub fn delete_smart_module(&self, name: String) -> PyResult<()> {
        delete_impl!(self, name, NativeSmartModuleSpec)
    }

    pub fn list_smart_modules(
        &self,
        filters: Vec<String>,
    ) -> PyResult<Vec<MetadataSmartModuleSpec>> {
        list_impl!(self, filters)
    }

    pub fn watch_smart_module(&self) -> PyResult<WatchSmartModuleStream> {
        watch_impl!(self, WatchSmartModuleStream)
    }

    pub fn list_partitions(&self, filters: Vec<String>) -> PyResult<Vec<MetadataPartitionSpec>> {
        list_impl!(self, filters)
    }
}

#[derive(Clone)]
#[pyclass]
struct TopicSpec {
    inner: NativeTopicSpec,
}

#[pymethods]
impl TopicSpec {
    #[staticmethod]
    pub fn new_assigned(maps: Vec<PartitionMap>) -> TopicSpec {
        TopicSpec {
            inner: NativeTopicSpec::new_assigned(into_native_partition_maps(maps)),
        }
    }

    #[staticmethod]
    pub fn new_computed(partitions: u32, replications: u32, ignore: Option<bool>) -> TopicSpec {
        TopicSpec {
            inner: NativeTopicSpec::new_computed(partitions, replications, ignore),
        }
    }

    #[staticmethod]
    pub fn new_mirror() -> TopicSpec {
        let mut home_mirror = HomeMirrorConfig::from(vec![]);
        //TODO: when update to 0.13.0
        //home_mirror.source = home_to_remote;
        let mirror_map = MirrorConfig::Home(home_mirror);
        TopicSpec {
            inner: NativeTopicSpec::new_mirror(mirror_map),
        }
    }

    pub fn set_partition(&mut self, system: bool) {
        self.inner.set_system(system);
    }

    pub fn set_system(&mut self, system: bool) {
        self.inner.set_system(system);
    }

    pub fn set_retention_time(&mut self, time_in_seconds: i64) {
        self.inner
            .set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
                time_in_seconds: time_in_seconds as u32,
            }));
    }

    pub fn set_storage(&mut self, max_partition_size: Option<i64>, segment_size: Option<i64>) {
        let mut storage = TopicStorageConfig::default();

        if let Some(segment_size) = segment_size {
            storage.segment_size = Some(segment_size as u32);
        }

        if let Some(max_partition_size) = max_partition_size {
            storage.max_partition_size = Some(max_partition_size as u64);
        }

        self.inner.set_storage(storage);
    }

    pub fn set_compression_type(&mut self, compression: &str) -> PyResult<()> {
        let compression = match compression {
            "none" => CompressionAlgorithm::None,
            "gzip" => CompressionAlgorithm::Gzip,
            "snappy" => CompressionAlgorithm::Snappy,
            "lz4" => CompressionAlgorithm::Lz4,
            "zstd" => CompressionAlgorithm::Zstd,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid compression type: {}",
                    compression
                )))
            }
        };

        self.inner.set_compression_type(compression);
        Ok(())
    }
}

fn into_native_partition_maps(maps: Vec<PartitionMap>) -> Vec<NativePartitionMap> {
    maps.into_iter().map(|map| map.into()).collect()
}

#[derive(Clone)]
#[pyclass]
struct PartitionMap {
    inner: NativePartitionMap,
}

#[pymethods]
impl PartitionMap {
    #[staticmethod]
    fn new(partition: NativePartitionId, replicas: Vec<NativeSpuId>) -> Self {
        PartitionMap {
            inner: NativePartitionMap {
                id: partition,
                replicas,
                ..Default::default()
            },
        }
    }
}

impl From<PartitionMap> for NativePartitionMap {
    fn from(val: PartitionMap) -> Self {
        val.inner
    }
}

#[derive(Clone)]
#[pyclass]
struct CommonCreateRequest {
    inner: NativeCommonCreateRequest,
}

#[pymethods]
impl CommonCreateRequest {
    #[staticmethod]
    pub fn new(name: String, dry_run: bool, timeout: Option<u32>) -> CommonCreateRequest {
        CommonCreateRequest {
            inner: NativeCommonCreateRequest {
                name,
                dry_run,
                timeout,
            },
        }
    }
}

#[pyclass]
struct MetadataTopicSpec {
    inner: NativeMetadata<NativeTopicSpec>,
}

#[pymethods]
impl MetadataTopicSpec {
    fn name(&self) -> String {
        self.inner.name.clone()
    }
}

impl From<NativeMetadata<NativeTopicSpec>> for MetadataTopicSpec {
    fn from(data: NativeMetadata<NativeTopicSpec>) -> Self {
        MetadataTopicSpec { inner: data }
    }
}

#[pyclass]
struct MetaUpdateTopicSpec {
    inner: NativeMetadataUpdate<NativeTopicSpec>,
}

#[pymethods]
impl MetaUpdateTopicSpec {
    fn epoch(&self) -> i64 {
        self.inner.epoch
    }
    fn changes(&self) -> Vec<MessageMetadataTopicSpec> {
        self.inner
            .changes
            .clone()
            .into_iter()
            .map(|s| s.into())
            .collect()
    }
    fn all(&self) -> Vec<MetadataTopicSpec> {
        self.inner
            .all
            .clone()
            .into_iter()
            .map(|s| s.into())
            .collect()
    }
}

#[pyclass]
struct MessageMetadataTopicSpec {
    inner: NativeMessage<NativeMetadata<NativeTopicSpec>>,
}

#[pymethods]
impl MessageMetadataTopicSpec {
    fn is_update(&self) -> bool {
        matches!(&self.inner.header, NativeMsgType::UPDATE)
    }

    fn is_delete(&self) -> bool {
        matches!(&self.inner.header, NativeMsgType::DELETE)
    }

    fn metadata_topic_spec(&self) -> MetadataTopicSpec {
        self.inner.content.clone().into()
    }
}

impl From<NativeMessage<NativeMetadata<NativeTopicSpec>>> for MessageMetadataTopicSpec {
    fn from(data: NativeMessage<NativeMetadata<NativeTopicSpec>>) -> Self {
        MessageMetadataTopicSpec { inner: data }
    }
}

#[pyclass]
struct WatchResponseTopicSpec {
    inner: NativeWatchResponse<NativeTopicSpec>,
}

#[pymethods]
impl WatchResponseTopicSpec {
    fn inner(&self) -> MetaUpdateTopicSpec {
        MetaUpdateTopicSpec {
            inner: self.inner.clone().inner(),
        }
    }
}

impl From<NativeWatchResponse<NativeTopicSpec>> for WatchResponseTopicSpec {
    fn from(data: NativeWatchResponse<NativeTopicSpec>) -> Self {
        WatchResponseTopicSpec { inner: data }
    }
}

#[pyclass]
struct MetaUpdateSmartModuleSpec {
    inner: NativeMetadataUpdate<NativeSmartModuleSpec>,
}

#[pymethods]
impl MetaUpdateSmartModuleSpec {
    fn epoch(&self) -> i64 {
        self.inner.epoch
    }
    fn changes(&self) -> Vec<MessageMetadataSmartModuleSpec> {
        self.inner
            .changes
            .clone()
            .into_iter()
            .map(|s| s.into())
            .collect()
    }
    fn all(&self) -> Vec<MetadataSmartModuleSpec> {
        self.inner
            .all
            .clone()
            .into_iter()
            .map(|s| s.into())
            .collect()
    }
}

#[pyclass]
struct MessageMetadataSmartModuleSpec {
    inner: NativeMessage<NativeMetadata<NativeSmartModuleSpec>>,
}

#[pymethods]
impl MessageMetadataSmartModuleSpec {
    fn is_update(&self) -> bool {
        matches!(&self.inner.header, NativeMsgType::UPDATE)
    }

    fn is_delete(&self) -> bool {
        matches!(&self.inner.header, NativeMsgType::DELETE)
    }

    fn metadata_smart_module_spec(&self) -> MetadataSmartModuleSpec {
        self.inner.content.clone().into()
    }
}

impl From<NativeMessage<NativeMetadata<NativeSmartModuleSpec>>> for MessageMetadataSmartModuleSpec {
    fn from(data: NativeMessage<NativeMetadata<NativeSmartModuleSpec>>) -> Self {
        MessageMetadataSmartModuleSpec { inner: data }
    }
}

type WatchTopicIteratorInner =
    Pin<Box<dyn Stream<Item = Result<NativeWatchResponse<NativeTopicSpec>, IoError>> + Send>>;

#[pyclass(unsendable)]
pub struct WatchTopicStream {
    pub inner: WatchTopicIteratorInner,
}

impl WatchTopicStream {
    pub fn new(inner: WatchTopicIteratorInner) -> Self {
        WatchTopicStream { inner }
    }
}

#[pymethods]
impl WatchTopicStream {
    fn next(&mut self) -> Result<Option<WatchResponseTopicSpec>, PyErr> {
        Ok(Some(WatchResponseTopicSpec {
            inner: run_block_on(self.inner.next())
                .unwrap()
                .map_err(|err| PyException::new_err(err.to_string()))?,
        }))
    }
}

#[derive(Clone)]
#[pyclass]
struct SmartModuleSpec {
    inner: NativeSmartModuleSpec,
}

#[pymethods]
impl SmartModuleSpec {
    #[staticmethod]
    fn with_binary(bytes: Vec<u8>) -> Self {
        SmartModuleSpec {
            inner: NativeSmartModuleSpec {
                wasm: NativeSmartModuleWasm {
                    format: NativeSmartModuleWasmFormat::Binary,
                    payload: bytes.into(),
                },
                ..Default::default()
            },
        }
    }
}

#[pyclass]
struct MetadataSmartModuleSpec {
    inner: NativeMetadata<NativeSmartModuleSpec>,
}

#[pymethods]
impl MetadataSmartModuleSpec {
    fn name(&self) -> String {
        self.inner.name.clone()
    }
}

impl From<NativeMetadata<NativeSmartModuleSpec>> for MetadataSmartModuleSpec {
    fn from(data: NativeMetadata<NativeSmartModuleSpec>) -> Self {
        MetadataSmartModuleSpec { inner: data }
    }
}

#[pyclass]
struct WatchResponseSmartModuleSpec {
    inner: NativeWatchResponse<NativeSmartModuleSpec>,
}

#[pymethods]
impl WatchResponseSmartModuleSpec {
    fn inner(&self) -> MetaUpdateSmartModuleSpec {
        MetaUpdateSmartModuleSpec {
            inner: self.inner.clone().inner(),
        }
    }
}

impl From<NativeWatchResponse<NativeSmartModuleSpec>> for WatchResponseSmartModuleSpec {
    fn from(data: NativeWatchResponse<NativeSmartModuleSpec>) -> Self {
        WatchResponseSmartModuleSpec { inner: data }
    }
}

type WatchSmartModuleIteratorInner =
    Pin<Box<dyn Stream<Item = Result<NativeWatchResponse<NativeSmartModuleSpec>, IoError>> + Send>>;

#[pyclass(unsendable)]
pub struct WatchSmartModuleStream {
    pub inner: WatchSmartModuleIteratorInner,
}

impl WatchSmartModuleStream {
    pub fn new(inner: WatchSmartModuleIteratorInner) -> Self {
        WatchSmartModuleStream { inner }
    }
}

#[pymethods]
impl WatchSmartModuleStream {
    fn next(&mut self) -> Result<Option<WatchResponseSmartModuleSpec>, PyErr> {
        Ok(Some(WatchResponseSmartModuleSpec {
            inner: run_block_on(self.inner.next())
                .unwrap()
                .map_err(|err| PyException::new_err(err.to_string()))?,
        }))
    }
}

#[pyclass]
struct PartitionSpec {
    inner: NativePartitionSpec,
}

impl From<NativePartitionSpec> for PartitionSpec {
    fn from(data: NativePartitionSpec) -> Self {
        PartitionSpec { inner: data }
    }
}

#[pyclass]
struct MetadataPartitionSpec {
    inner: NativeMetadata<NativePartitionSpec>,
}

#[pymethods]
impl MetadataPartitionSpec {
    fn name(&self) -> String {
        self.inner.name.clone()
    }
}

impl From<NativeMetadata<NativePartitionSpec>> for MetadataPartitionSpec {
    fn from(data: NativeMetadata<NativePartitionSpec>) -> Self {
        MetadataPartitionSpec { inner: data }
    }
}

/// Runs the given future to completion, checking for Python signals periodically.
fn run_future_checking_signals<'a, T>(
    future: impl Future<Output = T> + Send + 'a,
    py: Python<'a>,
) -> PyResult<T> {
    // Pin the future to ensure it has a stable address in memory
    let mut future_pin = Box::pin(future);

    // Define the asynchronous task that handles the future and signal checking
    let res = async {
        loop {
            select! {
                // Poll the pinned future by mutable reference
                res = &mut future_pin => {
                    return Ok(res);
                }

                // Sleep for 100 ms to periodically check for signals
                _ = sleep(Duration::from_millis(100)) => {
                    // Check for Python signals (like KeyboardInterrupt)
                    py.check_signals()?;
                }
            }
        }
    };

    // Run the asynchronous task to completion
    run_block_on(res)
}
