#![allow(non_snake_case, unused)]
use flate2::bufread::GzEncoder;
use flate2::Compression;
use fluvio::consumer::{
    ConsumerConfig as NativeConsumerConfig, ConsumerConfigBuilder, SmartModuleInvocation,
    SmartModuleInvocationWasm, SmartModuleKind as NativeSmartModuleKind,
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

pub struct ConsumerConfig {
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

    pub fn wasm_module_path(
        &mut self,
        wasm_module_path: &str,
        kind: SmartModuleKind,
    ) -> Result<(), FluvioError> {
        let wasm_module_buffer = std::fs::read(wasm_module_path)?;
        let mut encoder = GzEncoder::new(wasm_module_buffer.as_slice(), Compression::default());
        let mut buffer = Vec::with_capacity(wasm_module_buffer.len());
        let encoder = encoder.read_to_end(&mut buffer)?;
        self.builder.smartmodule(vec![SmartModuleInvocation {
            wasm: SmartModuleInvocationWasm::AdHoc(wasm_module_buffer),
            kind: kind.into(),
            params: Default::default(),
        }]);

        Ok(())
    }
}

impl Into<NativeSmartModuleKind> for SmartModuleKind {
    fn into(self) -> NativeSmartModuleKind {
        match self {
            SmartModuleKind::Filter => NativeSmartModuleKind::Filter,
            SmartModuleKind::Map => NativeSmartModuleKind::Map,
            SmartModuleKind::ArrayMap => NativeSmartModuleKind::ArrayMap,
            SmartModuleKind::FilterMap => NativeSmartModuleKind::FilterMap,
            _ => NativeSmartModuleKind::default(), // default is Filter.
        }
    }
}

#[derive(Clone)]
pub enum SmartModuleKind {
    Filter,
    Map,
    ArrayMap,
    FilterMap,
    /*
    Join(String),
    JoinStream {
        topic: String,
        derivedstream: String,
    },
    Aggregate {
        accumulator: Vec<u8>,
    },
    Generic(SmartModuleContextData),
    */
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
        config: &ConsumerConfig,
    ) -> Result<PartitionConsumerStream, FluvioError> {
        let config = config.builder.build()?;

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
    use std::io;
    use std::io::Write;
    use tracing::info;
    use url::Host;
    const DEFAULT_PROFILE_NAME: &'static str = "cloud";
    use crate::cloud::{Auth0Config, DeviceCodeResponse};
    pub struct CloudAuth {
        pub auth0_config: Option<Auth0Config>,
        pub device_code: Option<DeviceCodeResponse>,
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
                None => rpassword::read_password_from_tty(Some("Password: "))?,
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
