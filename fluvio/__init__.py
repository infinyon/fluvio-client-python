"""
The __fluvio__ python module provides an extension for working with the Fluvio
streaming platform.

This module builds on top of the Fluvio Client Rust Crate and provides a
pythonic access to the API.


Creating a topic with default settings is as simple as:

```python
fluvio_admin = FluvioAdmin.connect()
fluvio_admin.create_topic("a_topic")
```

Or just create a topic with custom settings:

```python
import fluvio

fluvio_admin = FluvioAdmin.connect()
topic_spec = (
    TopicSpec.create()
    .with_retention_time("1h")
    .with_segment_size("10M")
    .build()
)
fluvio_admin.create_topic("a_topic", topic_spec)
```

Producing data to a topic in a Fluvio cluster is as simple as:

```python
import fluvio

fluvio = Fluvio.connect()

topic = "a_topic"
producer = fluvio.topic_producer(topic)

for i in range(10):
    producer.send_string("Hello %s " % i)
```

Consuming is also simple:

```python
import fluvio

fluvio = Fluvio.connect()

topic = "a_topic"
builder = ConsumerConfigExtBuilder(topic)
config = builder.build()
stream = fluvio.consumer_with_config(config)

num_items = 2
records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]
```

Producing with a custom configuration:

```python
import fluvio

fluvio = Fluvio.connect()

topic = "a_topic"
builder = (
    TopicProducerConfigBuilder()
    .batch_size(32768)
    .linger(100)
    .compression(Compression.Gzip)
    .max_request_size(33554432)
    .timeout(600000)
    .isolation(Isolation.ReadCommitted)
    .delivery_semantic(DeliverySemantic.AtLeastOnce)
)
producer = fluvio.topic_producer_with_config(self.topic, config)

for i in range(10):
    producer.send_string("Hello %s " % i)
```

Also you can consume usign offset management:

```python
import fluvio

fluvio = Fluvio.connect()

topic = "a_topic"
builder = ConsumerConfigExtBuilder(topic)
builder.offset_start(Offset.beginning())
builder.offset_strategy(OffsetManagementStrategy.MANUAL)
builder.offset_consumer("a-consumer")
config = builder.build()
stream = fluvio.consumer_with_config(config)

num_items = 2
records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]

stream.offset_commit()
stream.offset_flush()
```

For more examples see the integration tests in the fluvio-python repository.

[test_produce.py](https://github.com/infinyon/fluvio-client-python/blob/main/integration-tests/test_produce.py)
[test_consumer.py](https://github.com/infinyon/fluvio-client-python/blob/main/integration-tests/test_consume.py)

"""

import typing
from enum import Enum

from ._fluvio_python import (
    Fluvio as _Fluvio,
    FluvioConfig as _FluvioConfig,
    Offset,
    FluvioAdmin as _FluvioAdmin,
    # consumer types
    ConsumerConfig as _ConsumerConfig,
    ConsumerConfigExt,
    ConsumerConfigExtBuilder,
    PartitionConsumer as _PartitionConsumer,
    MultiplePartitionConsumer as _MultiplePartitionConsumer,
    PartitionSelectionStrategy as _PartitionSelectionStrategy,
    PartitionConsumerStream as _PartitionConsumerStream,
    AsyncPartitionConsumerStream as _AsyncPartitionConsumerStream,
    OffsetManagementStrategy,
    ConsumerOffset as _ConsumerOffset,
    # producer types
    TopicProducer as _TopicProducer,
    TopicProducerConfig,
    TopicProducerConfigBuilder,
    Compression,
    DeliverySemantic,
    Isolation,
    ProduceOutput as _ProduceOutput,
    ProducerBatchRecord as _ProducerBatchRecord,
    # admin and misc types
    SmartModuleKind as _SmartModuleKind,
    TopicSpec as _TopicSpec,
    WatchTopicStream as _WatchTopicStream,
    WatchSmartModuleStream as _WatchSmartModuleStream,
)

from ._fluvio_python import Error as FluviorError  # noqa: F401

from .topic import TopicSpec, CompressionType, TopicMode
from .record import Record, RecordMetadata
from .specs import (
    # support types
    CommonCreateRequest,
    PartitionMap,
    # specs
    SmartModuleSpec,
    MessageMetadataTopicSpec,
    MetadataPartitionSpec,
    MetadataSmartModuleSpec,
    MetadataTopicSpec,
    MetaUpdateSmartModuleSpec,
    MetaUpdateTopicSpec,
)

# this structures the module a bit and allows pydoc to generate better docs
# with better ordering of types and functions
# inclusion in __all__, will pull the PyO3 rust inline docs into
# the pdoc generated documentation
__all__ = [
    # top level types
    "Fluvio",
    "FluvioConfig",
    "FluvioAdmin",
    # topic
    "TopicSpec",
    "CompressionType",
    "TopicMode",
    # record
    "Record",
    "RecordMetadata",
    "Offset",
    # producer
    "TopicProducer",
    "TopicProducerConfig",
    "TopicProducerConfigBuilder",
    "Compression",
    "DeliverySemantic",
    "Isolation",
    "ProduceOutput",
    # consumer
    "ConsumerConfigExt",
    "ConsumerConfigExtBuilder",
    "ConsumerConfig",
    "PartitionConsumer",
    "MultiplePartitionConsumer",
    "OffsetManagementStrategy",
    "ConsumerOffset",
    # specs
    "CommonCreateRequest",
    "SmartModuleSpec",
    "TopicSpec",
    "PartitionMap",
    "MessageMetadataTopicSpec",
    "MetadataPartitionSpec",
    "MetadataTopicSpec",
    "MetaUpdateTopicSpec",
    # Misc
    "PartitionSelectionStrategy",
    "SmartModuleKind",
]


class ProduceOutput:
    """Returned by of `TopicProducer.send` call allowing access to sent record metadata."""

    _inner: _ProduceOutput

    def __init__(self, inner: _ProduceOutput) -> None:
        self._inner = inner

    def wait(self) -> typing.Optional[RecordMetadata]:
        """Wait for the record metadata.

        This is a blocking call and may only return a `RecordMetadata` once.
        Any subsequent call to `wait` will return a `None` value.
        Errors will be raised as exceptions of type `FluvioError`.
        """
        res = self._inner.wait()
        if res is None:
            return None
        return RecordMetadata(res)

    async def async_wait(self) -> typing.Optional[RecordMetadata]:
        """Asynchronously wait for the record metadata.

        This may only return a `RecordMetadata` once.
        Any subsequent call to `wait` will return a `None` value.
        """
        return await self._inner.async_wait()


class SmartModuleKind(Enum):
    """
    Use of this is to explicitly set the kind of a smartmodule. Not required
    but needed for legacy SmartModules.
    """

    Filter = _SmartModuleKind.Filter
    Map = _SmartModuleKind.Map
    ArrayMap = _SmartModuleKind.ArrayMap
    FilterMap = _SmartModuleKind.FilterMap
    Aggregate = _SmartModuleKind.Aggregate


class ConsumerConfig:
    _inner: _ConsumerConfig

    def __init__(self):
        self._inner = _ConsumerConfig()

    def disable_continuous(self, val: bool = True):
        """Disable continuous mode after fetching specified records"""
        self._inner.disable_continuous(val)

    def smartmodule(
        self,
        name: str = None,
        path: str = None,
        kind: SmartModuleKind = None,
        params: typing.Dict[str, str] = None,
        aggregate: typing.List[bytes] = None,
    ):
        """
        This is a method for adding a smartmodule to a consumer config either
        using a `name` of a `SmartModule` or a `path` to a wasm binary.

        Args:

            name: str
            path: str
            kind: SmartModuleKind
            params: Dict[str, str]
            aggregate: List[bytes] # This is used for the initial value of an aggregate smartmodule

        Raises:
            "Require either a path or a name for a smartmodule."
            "Only specify one of path or name not both."

        Returns:

            None
        """

        if kind is not None:
            kind = kind.value

        if path is None and name is None:
            raise Exception("Require either a path or a name for a smartmodule.")

        if path is not None and name is not None:
            raise Exception("Only specify one of path or name not both.")

        params = {} if params is None else params
        param_keys = [x for x in params.keys()]
        param_vals = [x for x in params.values()]

        self._inner.smartmodule(
            name,
            path,
            kind,
            param_keys,
            param_vals,
            aggregate,
            # These arguments are for Join stuff but that's not implemented on
            # the python side yet
            None,
            None,
            None,
            None,
        )


class ConsumerIterator:
    def __init__(self, stream: _PartitionConsumerStream):
        self.stream = stream

    def __iter__(self):
        return self

    def __next__(self):
        item = self.stream.next()
        if item is None:
            raise StopIteration
        return Record(item)

    def offset_commit(self):
        self.stream.offset_commit()

    def offset_flush(self):
        self.stream.offset_flush()


class PartitionConsumer:
    """
    An interface for consuming events from a particular partition

    There are two ways to consume events: by "fetching" events and by
    "streaming" events. Fetching involves specifying a range of events that you
    want to consume via their Offset. A fetch is a sort of one-time batch
    operation: you’ll receive all of the events in your range all at once. When
    you consume events via Streaming, you specify a starting Offset and receive
    an object that will continuously yield new events as they arrive.
    """

    _inner: _PartitionConsumer

    def __init__(self, inner: _PartitionConsumer):
        self._inner = inner

    def stream(self, offset: Offset) -> typing.Iterator[Record]:
        """
        Continuously streams events from a particular offset in the consumer’s
        partition. This returns a `Iterator[Record]` which is an
        iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.
        """
        return self._generator(self._inner.stream(offset))

    async def async_stream(self, offset: Offset) -> typing.AsyncIterator[Record]:
        """
        Continuously streams events from a particular offset in the consumer’s
        partition. This returns a `AsyncIterator[Record]` which is an
        iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.
        """
        return self._async_generator(await self._inner.async_stream(offset))

    def stream_with_config(
        self, offset: Offset, config: ConsumerConfig
    ) -> typing.Iterator[Record]:
        """
        Continuously streams events from a particular offset with a SmartModule
        WASM module in the consumer’s partition. This returns a
        `Iterator[Record]` which is an iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.

        Args:
            offset: Offset
            wasm_module_path: str - The absolute path to the WASM file

        Example:
            import os

            wmp = os.path.abspath("somefilter.wasm")
            config = ConsumerConfig()
            config.smartmodule(path=wmp)
            for i in consumer.stream_with_config(Offset.beginning(), config):
                # do something with i

        Returns:
            `Iterator[Record]`

        """
        return self._generator(self._inner.stream_with_config(offset, config._inner))

    async def async_stream_with_config(
        self, offset: Offset, config: ConsumerConfig
    ) -> typing.AsyncIterator[Record]:
        """
        Continuously streams events from a particular offset with a SmartModule
        WASM module in the consumer’s partition. This returns a
        `AsyncIterator[Record]` which is an async iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.

        Args:
            offset: Offset
            wasm_module_path: str - The absolute path to the WASM file

        Example:
            import os

            wmp = os.path.abspath("somefilter.wasm")
            config = ConsumerConfig()
            config.smartmodule(path=wmp)
            `AsyncIterator[Record]`

        """
        return self._async_generator(
            await self._inner.async_stream_with_config(offset, config._inner)
        )

    def _generator(self, stream: _PartitionConsumerStream) -> typing.Iterator[Record]:
        item = stream.next()
        while item is not None:
            yield Record(item)
            item = stream.next()

    async def _async_generator(
        self, astream: _AsyncPartitionConsumerStream
    ) -> typing.AsyncIterator[Record]:
        item = await astream.async_next()
        while item is not None:
            yield Record(item)
            item = await astream.async_next()


class MultiplePartitionConsumer:
    """
    An interface for consuming events from multiple partitions

    There are two ways to consume events: by "fetching" events and by
    "streaming" events. Fetching involves specifying a range of events that you
    want to consume via their Offset. A fetch is a sort of one-time batch
    operation: you’ll receive all of the events in your range all at once. When
    you consume events via Streaming, you specify a starting Offset and receive
    an object that will continuously yield new events as they arrive.
    """

    _inner: _MultiplePartitionConsumer

    def __init__(self, inner: _MultiplePartitionConsumer):
        self._inner = inner

    def stream(self, offset: Offset) -> typing.Iterator[Record]:
        """
        Continuously streams events from a particular offset in the consumer’s
        partitions. This returns a `Iterator[Record]` which is an
        iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.
        """
        return self._generator(self._inner.stream(offset))

    async def async_stream(self, offset: Offset) -> typing.AsyncIterator[Record]:
        """
        Continuously streams events from a particular offset in the consumer’s
        partitions. This returns a `AsyncIterator[Record]` which is an
        async iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.
        """
        return self._async_generator(await self._inner.async_stream(offset))

    def stream_with_config(
        self, offset: Offset, config: ConsumerConfig
    ) -> typing.Iterator[Record]:
        """
        Continuously streams events from a particular offset with a SmartModule
        WASM module in the consumer’s partitions. This returns a
        `Iterator[Record]` which is an iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.

        Args:
            offset: Offset
            wasm_module_path: str - The absolute path to the WASM file

        Example:
            import os

            wmp = os.path.abspath("somefilter.wasm")
            config = ConsumerConfig()
            config.smartmodule(path=wmp)
            for i in consumer.stream_with_config(Offset.beginning(), config):
                # do something with i

        Returns:
            `Iterator[Record]`

        """
        return self._generator(
            self._inner.stream_with_config(offset._inner, config._inner)
        )

    async def async_stream_with_config(
        self, offset: Offset, config: ConsumerConfig
    ) -> typing.AsyncIterator[Record]:
        """
        Continuously streams events from a particular offset with a SmartModule
        WASM module in the consumer’s partitions. This returns a
        `AsyncIterator[Record]` which is an async iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.

        Args:
            offset: Offset
            wasm_module_path: str - The absolute path to the WASM file

        Example:
            import os

            wmp = os.path.abspath("somefilter.wasm")
            config = ConsumerConfig()
            config.smartmodule(path=wmp)
            async for i in await consumer.async_stream_with_config(Offset.beginning(), config):
                # do something with i

        Returns:
            `AsyncIterator[Record]`

        """
        return self._async_generator(
            await self._inner.async_stream_with_config(offset, config._inner)
        )

    def _generator(self, stream: _PartitionConsumerStream) -> typing.Iterator[Record]:
        item = stream.next()
        while item is not None:
            yield Record(item)
            item = stream.next()

    async def _async_generator(
        self, astream: _AsyncPartitionConsumerStream
    ) -> typing.AsyncIterator[Record]:
        item = await astream.async_next()
        while item is not None:
            yield Record(item)
            item = await astream.async_next()


class TopicProducer:
    """An interface for producing events to a particular topic.

    A `TopicProducer` allows you to send events to the specific topic it was
    initialized for. Once you have a `TopicProducer`, you can send events to
    the topic, choosing which partition each event should be delivered to.
    """

    _inner: _TopicProducer

    def __init__(self, inner: _TopicProducer):
        self._inner = inner

    def send_string(self, buf: str) -> ProduceOutput:
        """Sends a string to this producer’s topic"""
        return self.send([], buf.encode("utf-8"))

    async def async_send_string(self, buf: str) -> ProduceOutput:
        """Sends a string to this producer’s topic"""
        return await self.async_send([], buf.encode("utf-8"))

    def send(self, key: typing.List[int], value: typing.List[int]) -> ProduceOutput:
        """
        Sends a key/value record to this producer's Topic.

        The partition that the record will be sent to is derived from the Key.
        """
        return ProduceOutput(self._inner.send(key, value))

    async def async_send(
        self, key: typing.List[int], value: typing.List[int]
    ) -> ProduceOutput:
        """
        Async sends a key/value record to this producer's Topic.

        The partition that the record will be sent to is derived from the Key.
        """
        produce_output = await self._inner.async_send(key, value)
        return ProduceOutput(produce_output)

    def flush(self) -> None:
        """
        Send all the queued records in the producer batches.
        """
        return self._inner.flush()

    async def async_flush(self) -> None:
        """
        Async send all the queued records in the producer batches.
        """
        await self._inner.async_flush()

    def send_all(
        self, records: typing.List[typing.Tuple[bytes, bytes]]
    ) -> typing.List[ProduceOutput]:
        """
        Sends a list of key/value records as a batch to this producer's Topic.
        :param records: The list of records to send
        """
        records_inner = [_ProducerBatchRecord(x, y) for (x, y) in records]
        return [
            ProduceOutput(output_inner)
            for output_inner in self._inner.send_all(records_inner)
        ]

    async def async_send_all(
        self, records: typing.List[typing.Tuple[bytes, bytes]]
    ) -> typing.List[ProduceOutput]:
        """
        Async sends a list of key/value records as a batch to this producer's Topic.
        :param records: The list of records to send
        """
        records_inner = [_ProducerBatchRecord(x, y) for (x, y) in records]
        return [
            ProduceOutput(output_inner)
            for output_inner in await self._inner.async_send_all(records_inner)
        ]


class PartitionSelectionStrategy:
    """Stragegy to select partitions"""

    _inner: _PartitionSelectionStrategy

    def __init__(self, inner: _FluvioConfig):
        self._inner = inner

    @classmethod
    def with_all(cls, topic: str):
        """select all partitions of one topic"""
        return cls(_PartitionSelectionStrategy.with_all(topic))

    @classmethod
    def with_multiple(cls, topic: typing.List[typing.Tuple[str, int]]):
        """select multiple partitions of multiple topics"""
        return cls(_PartitionSelectionStrategy.with_multiple(topic))


class ConsumerOffset:
    """Consumer offset"""

    _inner: _ConsumerOffset

    def __init__(self, inner: _ConsumerOffset):
        self._inner = inner

    def consumer_id(self) -> str:
        return self._inner.consumer_id()

    def topic(self) -> str:
        return self._inner.topic()

    def partition(self) -> int:
        return self._inner.partition()

    def offset(self) -> int:
        return self._inner.offset()

    def modified_time(self) -> int:
        return self._inner.modified_time()


class FluvioConfig:
    """Configuration for Fluvio client"""

    _inner: _FluvioConfig

    def __init__(self, inner: _FluvioConfig):
        self._inner = inner

    @classmethod
    def load(cls):
        """get current cluster config from default profile"""
        return cls(_FluvioConfig.load())

    @classmethod
    def new(cls, addr: str):
        """Create a new cluster configuration with no TLS."""
        return cls(_FluvioConfig.new(addr))

    def set_endpoint(self, endpoint: str):
        """set endpoint"""
        self._inner.set_endpoint(endpoint)

    def set_use_spu_local_address(self, val: bool):
        """set wheather to use spu local address"""
        self._inner.set_use_spu_local_address(val)

    def disable_tls(self):
        """disable tls for this config"""
        self._inner.disable_tls()

    def set_anonymous_tls(self):
        """set the config to use anonymous tls"""
        self._inner.set_anonymous_tls()

    def set_inline_tls(self, domain: str, key: str, cert: str, ca_cert: str):
        """specify inline tls parameters"""
        self._inner.set_inline_tls(domain, key, cert, ca_cert)

    def set_tls_file_paths(
        self, domain: str, key_path: str, cert_path: str, ca_cert_path: str
    ):
        """specify paths to tls files"""
        self._inner.set_tls_file_paths(domain, key_path, cert_path, ca_cert_path)

    def set_client_id(self, client_id: str):
        """set client id"""
        self._inner.set_client_id(client_id)

    def unset_client_id(self):
        """remove the configured client id from config"""
        self._inner.unset_client_id()


class Fluvio:
    """An interface for interacting with Fluvio streaming."""

    _inner: _Fluvio

    def __init__(self, inner: _Fluvio):
        self._inner = inner

    @classmethod
    def connect(cls):
        """Tries to create a new Fluvio client using the current profile from
        `~/.fluvio/config`
        """
        return cls(_Fluvio.connect())

    @classmethod
    def connect_with_config(cls, config: FluvioConfig):
        """Creates a new Fluvio client using the given configuration"""
        return cls(_Fluvio.connect_with_config(config._inner))

    def consumer_with_config(self, config: ConsumerConfigExt) -> ConsumerIterator:
        """Creates consumer with settings defined in config

        This is the recommended way to create a consume records.
        """
        return ConsumerIterator(self._inner.consumer_with_config(config))

    def topic_producer_with_config(self, topic: str, config: TopicProducerConfig):
        """Creates a new `TopicProducer` for the given topic name with config"""
        return TopicProducer(self._inner.topic_producer_with_config(topic, config))

    def topic_producer(self, topic: str) -> TopicProducer:
        """
        Creates a new `TopicProducer` for the given topic name.

        Currently, producers are scoped to a specific Fluvio topic. That means
        when you send events via a producer, you must specify which partition
        each event should go to.
        """
        return TopicProducer(self._inner.topic_producer(topic))

    def partition_consumer(self, topic: str, partition: int) -> PartitionConsumer:
        """Creates a new `PartitionConsumer` for the given topic and partition

        Currently, consumers are scoped to both a specific Fluvio topic and to
        a particular partition within that topic. That means that if you have a
        topic with multiple partitions, then in order to receive all of the
        events in all of the partitions, you will need to create one consumer
        per partition.
        """
        return PartitionConsumer(self._inner.partition_consumer(topic, partition))

    def multi_partition_consumer(self, topic: str) -> MultiplePartitionConsumer:
        """Creates a new `MultiplePartitionConsumer` for the given topic and its all partitions

        Currently, consumers are scoped to both a specific Fluvio topic and to
        its all partitions within that topic.
        """
        strategy = PartitionSelectionStrategy.with_all(topic)
        return MultiplePartitionConsumer(
            self._inner.multi_partition_consumer(strategy._inner)
        )

    def multi_topic_partition_consumer(
        self, selections: typing.List[typing.Tuple[str, int]]
    ) -> MultiplePartitionConsumer:
        """Creates a new `MultiplePartitionConsumer` for the given topics and partitions

        Currently, consumers are scoped to a list of Fluvio topic and partition tuple.
        """
        strategy = PartitionSelectionStrategy.with_multiple(selections)
        return MultiplePartitionConsumer(
            self._inner.multi_partition_consumer(strategy._inner)
        )

    def consumer_offsets(self) -> typing.List[ConsumerOffset]:
        """Fetch the current offsets of the consumer"""
        return self._inner.consumer_offsets()

    def delete_consumer_offset(self, consumer: str, topic: str, partition: int):
        """Delete the consumer offset"""
        return self._inner.delete_consumer_offset(consumer, topic, partition)

    def _generator(self, stream: _PartitionConsumerStream) -> typing.Iterator[Record]:
        item = stream.next()
        while item is not None:
            yield Record(item)
            item = stream.next()


class FluvioAdmin:
    _inner: _FluvioAdmin

    def __init__(self, inner: _FluvioAdmin):
        self._inner = inner

    def connect():
        return FluvioAdmin(_FluvioAdmin.connect())

    def connect_with_config(config: FluvioConfig):
        return FluvioAdmin(_FluvioAdmin.connect_with_config(config._inner))

    def create_topic(self, topic: str, spec: typing.Optional[_TopicSpec] = None):
        partitions = 1
        replication = 1
        ignore_rack = True
        dry_run = False
        spec = (
            spec
            if spec is not None
            else _TopicSpec.new_computed(partitions, replication, ignore_rack)
        )
        return self._inner.create_topic(topic, dry_run, spec)

    def create_topic_with_config(
        self, topic: str, req: CommonCreateRequest, spec: _TopicSpec
    ):
        return self._inner.create_topic_with_config(topic, req._inner, spec)

    def delete_topic(self, topic: str):
        return self._inner.delete_topic(topic)

    def all_topics(self) -> typing.List[MetadataTopicSpec]:
        return self._inner.all_topics()

    def list_topics(self, filters: typing.List[str]) -> typing.List[MetadataTopicSpec]:
        return self._inner.list_topics(filters)

    def list_topics_with_params(
        self, filters: typing.List[str], summary: bool
    ) -> typing.List[MetadataTopicSpec]:
        return self._inner.list_topics_with_params(filters, summary)

    def watch_topic(self) -> typing.Iterator[MetadataTopicSpec]:
        return self._topic_spec_generator(self._inner.watch_topic())

    def create_smartmodule(self, name: str, path: str, dry_run: bool):
        spec = SmartModuleSpec.new(path)
        return self._inner.create_smart_module(name, dry_run, spec._inner)

    def delete_smartmodule(self, name: str):
        return self._inner.delete_smart_module(name)

    def list_smartmodules(
        self, filters: typing.List[str]
    ) -> typing.List[MetadataSmartModuleSpec]:
        return self._inner.list_smart_modules(filters)

    def watch_smartmodule(self) -> typing.Iterator[MetadataSmartModuleSpec]:
        return self._smart_module_spec_generator(self._inner.watch_smart_module())

    def list_partitions(
        self, filters: typing.List[str]
    ) -> typing.List[MetadataPartitionSpec]:
        return self._inner.list_partitions(filters)

    def _topic_spec_generator(
        self, stream: _WatchTopicStream
    ) -> typing.Iterator[MetaUpdateTopicSpec]:
        item = stream.next().inner()
        while item is not None:
            yield MetaUpdateTopicSpec(item)
            item = stream.next().inner()

    def _smart_module_spec_generator(
        self, stream: _WatchSmartModuleStream
    ) -> typing.Iterator[MetaUpdateSmartModuleSpec]:
        item = stream.next().inner()
        while item is not None:
            yield MetaUpdateSmartModuleSpec(item)
            item = stream.next().inner()
