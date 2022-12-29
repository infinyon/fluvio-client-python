#![allow(non_snake_case, unused)]
use flate2::bufread::GzEncoder;
use flate2::Compression;
use fluvio::consumer::ConsumerConfig;
use fluvio::consumer::{SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind};
use fluvio::dataplane::link::ErrorCode;
use fluvio::{consumer::Record, Fluvio, FluvioError, Offset, PartitionConsumer, TopicProducer};
use fluvio_future::{
    io::{Stream, StreamExt},
    task::run_block_on,
};
use std::io::{Error, Read};
use std::pin::Pin;
use std::string::FromUtf8Error;
mod cloud;
use cloud::{CloudClient, CloudLoginError};

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

pub struct ConsumerConfigWrapper {
    wasm_module: Vec<u8>,
}

impl ConsumerConfigWrapper {
    fn new_config_with_wasm_filter(file: &str) -> Result<ConsumerConfigWrapper, std::io::Error> {
        let raw_buffer = std::fs::read(file)?;
        let mut encoder = GzEncoder::new(raw_buffer.as_slice(), Compression::default());
        let mut buffer = Vec::with_capacity(raw_buffer.len());
        let encoder = encoder.read_to_end(&mut buffer)?;
        Ok(ConsumerConfigWrapper {
            wasm_module: buffer,
        })
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
        wasm_module_path: &str,
    ) -> Result<PartitionConsumerStream, FluvioError> {
        let config_wrapper = ConsumerConfigWrapper::new_config_with_wasm_filter(wasm_module_path)?;
        let mut builder = ConsumerConfig::builder();
        builder.smartmodule(vec![SmartModuleInvocation {
            wasm: SmartModuleInvocationWasm::AdHoc(config_wrapper.wasm_module),
            kind: SmartModuleKind::Filter,
            params: Default::default(),
        }]);
        let config = builder
            .build()
            .map_err(|err| FluvioError::Other(err.to_string()))?;
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

mod _Cloud {
    use super::*;
    use fluvio::config::{ConfigFile, FluvioConfig, Profile};
    use tracing::info;
    use url::Host;
    const DEFAULT_PROFILE_NAME: &'static str = "cloud";

    pub fn login(
        use_oauth2: bool,
        remote: String,
        profile: Option<String>,
        email: Option<String>,
        password: Option<String>,
    ) -> Result<(), CloudLoginError> {
        run_block_on(async {
            let mut client = CloudClient::with_default_path()?;
            if use_oauth2 {
                client.authenticate_with_auth0(remote.as_str()).await?;
            } else {
                use std::io;
                use std::io::Write;
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
                    None => rpassword::read_password_from_tty(Some("Password: "))?,
                };
                client.authenticate(email, &password, &remote).await?;
            }

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

    fn save_cluster(
        cluster: FluvioConfig,
        remote: String,
        profile: Option<String>,
    ) -> Result<(), CloudLoginError> {
        let mut config_file = ConfigFile::load_default_or_new()?;
        let config = config_file.mut_config();
        let profile_name = if let Some(profile) = profile {
            profile
        } else {
            profile_from_remote(remote).unwrap_or_else(|| DEFAULT_PROFILE_NAME.to_string())
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
}

include!(concat!(env!("OUT_DIR"), "/glue.rs"));
