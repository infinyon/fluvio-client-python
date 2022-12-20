#![allow(non_snake_case, unused)]
use flate2::bufread::GzEncoder;
use flate2::Compression;
use fluvio::consumer::{
    ConsumerConfig as NativeConsumerConfig,
    ConsumerConfigBuilder,
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind,
};
use fluvio::dataplane::link::ErrorCode;
use fluvio::{consumer::Record, Fluvio, FluvioError, Offset, PartitionConsumer, TopicProducer};
use fluvio_future::{
    io::{Stream, StreamExt},
    task::run_block_on,
};
use std::io::{Error, Read};
use std::pin::Pin;
use std::string::FromUtf8Error;

mod _Fluvio {
    use super::*;
    pub fn connect() -> Result<Fluvio, FluvioError> {
        run_block_on(Fluvio::connect())
    }
    pub fn partition_consumer(
        fluvio: &Fluvio,
        topic: String,
        partition: u32,
    ) -> Result<PartitionConsumer, FluvioError> {
        run_block_on(fluvio.partition_consumer(topic, partition))
    }
    pub fn topic_producer(fluvio: &Fluvio, topic: String) -> Result<TopicProducer, FluvioError> {
        run_block_on(fluvio.topic_producer(topic))
    }
}

pub struct ConsumerConfig{
    pub(crate) builder: ConsumerConfigBuilder,
}

impl ConsumerConfig {
    fn new() -> Self {
        Self {
            builder: NativeConsumerConfig::builder(),
        }
    }
    pub fn max_bytes(&mut self, max_bytes: i32) {
        self.builder.max_bytes(max_bytes);
    }

    pub fn smartmodule(&mut self, smartmodules: Vec<SmartModuleInvocation>) {
        self.builder.smartmodule(smartmodules);
    }
}

mod _PartitionConsumer {
    use super::*;
    pub fn stream(
        consumer: &PartitionConsumer,
        offset: &Offset,
    ) -> Result<PartitionConsumerStream, FluvioError> {
        Ok(PartitionConsumerStream {
            inner: Box::pin(run_block_on(consumer.stream(offset.clone()))?),
        })
    }
    pub fn stream_with_config(
        consumer: &PartitionConsumer,
        offset: &Offset,
        config_wrapper: &ConsumerConfig,
    ) -> Result<PartitionConsumerStream, FluvioError> {
        let config = config_wrapper.builder.build().map_err(|err| FluvioError::Other(err.to_string()))?;

        run_block_on(consumer.stream_with_config(offset.clone(), config)).map(|stream| {
            PartitionConsumerStream {
                inner: Box::pin(stream),
            }
        })
    }
}

type PartitionConsumerIteratorInner = Pin<Box<dyn Stream<Item = Result<Record, ErrorCode>> + Send>>;

pub struct PartitionConsumerStream {
    pub inner: PartitionConsumerIteratorInner,
}
impl PartitionConsumerStream {
    pub fn next(&mut self) -> Option<Result<Record, ErrorCode>> {
        run_block_on(self.inner.next())
    }
}
#[derive(Clone)]
pub struct ProducerBatchRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
impl ProducerBatchRecord {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

mod _TopicProducer {
    use super::*;
    pub fn send(producer: &TopicProducer, key: &[u8], value: &[u8]) -> Result<(), FluvioError> {
        run_block_on(producer.send(key, value)).map(|_| ())
    }
    pub fn send_all(
        producer: &TopicProducer,
        records: &[ProducerBatchRecord],
    ) -> Result<(), FluvioError> {
        run_block_on(
            producer.send_all(records.iter().map(|record| -> (Vec<u8>, Vec<u8>) {
                (record.key.clone(), record.value.clone())
            })),
        )
        .map(|_| ())
    }
    pub fn flush(producer: &TopicProducer) -> Result<(), FluvioError> {
        run_block_on(producer.flush())
    }
}

mod _Record {
    use super::*;
    pub fn value_string(record: &Record) -> Result<String, FromUtf8Error> {
        String::from_utf8(record.value().to_vec())
    }
    pub fn key_string(record: &Record) -> Option<Result<String, FromUtf8Error>> {
        let key = record.key()?;
        Some(String::from_utf8(key.to_vec()))
    }
}

include!(concat!(env!("OUT_DIR"), "/glue.rs"));
