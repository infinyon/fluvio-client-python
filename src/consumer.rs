use pyo3::prelude::*;

use fluvio::consumer::{
    ConsumerConfigExt as NativeConsumerConfigExt,
    ConsumerConfigExtBuilder as NativeConsumerConfigExtBuilder,
    OffsetManagementStrategy as NativeOffsetManagementStrategy,
};
use fluvio::Offset as NativeOffset;

// types for consumer streams
use fluvio::consumer::{
    MultiplePartitionConsumerStream as NativeMultiplePartitionConsumerStreamGeneric,
    SinglePartitionConsumerStream as NativeSinglePartitionConsumerStreamGeneric,
};
use fluvio_protocol::link::ErrorCode;
use futures_util::stream::Stream;

use crate::Fluvio; // Python inner client type
use crate::FluvioError; // Python error mapping type
use crate::Offset; // python wrapped offset

#[pyclass]
#[derive(Clone)]
pub struct ConsumerConfigExt {
    pub inner: NativeConsumerConfigExt,
}

#[pyclass]
pub struct ConsumerConfigExtBuilder {
    builder: NativeConsumerConfigExtBuilder,
}

#[pymethods]
impl ConsumerConfigExtBuilder {
    #[new]
    fn new(topic: &str) -> Self {
        let mut builder = NativeConsumerConfigExtBuilder::default();
        builder.topic(topic);
        builder.offset_start(NativeOffset::beginning());
        Self { builder }
    }

    /// the fluvio client should disconnect after fetching
    /// the specified records
    #[pyo3(signature = (val = true))]
    fn disable_continuous(&mut self, val: bool) {
        self.builder.disable_continuous(val);
    }

    fn max_bytes(&mut self, max_bytes: i32) {
        self.builder.max_bytes(max_bytes);
    }

    fn offset_start(&mut self, offset: &Offset) {
        self.builder.offset_start(offset.0.clone());
    }

    fn offset_consumer(&mut self, id: &str) {
        self.builder.offset_consumer(id);
    }

    #[pyo3(signature = (strategy = OffsetManagementStrategy::AUTO))]
    fn offset_strategy(&mut self, strategy: OffsetManagementStrategy) {
        self.builder.offset_strategy(strategy.inner);
    }

    fn partition(&mut self, partition: u32) {
        self.builder.partition(partition);
    }

    fn topic(&mut self, topic: String) {
        self.builder.topic(topic);
    }

    fn build(&self) -> PyResult<ConsumerConfigExt> {
        let inner = self
            .builder
            .build()
            .map_err(|err| FluvioError::AnyhowError(err.into()))?;
        Ok(ConsumerConfigExt { inner })
    }
}

#[pyclass]
#[derive(Clone)]
pub struct OffsetManagementStrategy {
    inner: NativeOffsetManagementStrategy,
}

#[pymethods]
impl OffsetManagementStrategy {
    #[new]
    fn new() -> Self {
        Self {
            inner: NativeOffsetManagementStrategy::None,
        }
    }

    #[classattr]
    const NONE: OffsetManagementStrategy = OffsetManagementStrategy {
        inner: NativeOffsetManagementStrategy::None,
    };

    #[classattr]
    const MANUAL: OffsetManagementStrategy = OffsetManagementStrategy {
        inner: NativeOffsetManagementStrategy::Manual,
    };

    #[classattr]
    const AUTO: OffsetManagementStrategy = OffsetManagementStrategy {
        inner: NativeOffsetManagementStrategy::Auto,
    };
}
