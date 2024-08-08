from ._fluvio_python import (
    Fluvio as _Fluvio,
    FluvioConfig as _FluvioConfig,
    ConsumerConfig as _ConsumerConfig,
    PartitionConsumer as _PartitionConsumer,
    MultiplePartitionConsumer as _MultiplePartitionConsumer,
    PartitionSelectionStrategy as _PartitionSelectionStrategy,
    PartitionConsumerStream as _PartitionConsumerStream,
    AsyncPartitionConsumerStream as _AsyncPartitionConsumerStream,
    ProduceOutput as _ProduceOutput,
    RecordMetadata as _RecordMetadata,
    TopicProducer as _TopicProducer,
    ProducerBatchRecord as _ProducerBatchRecord,
    SmartModuleKind as _SmartModuleKind,
    Record as _Record,
    Offset as _Offset,
    FluvioAdmin as _FluvioAdmin,
    TopicSpec as _TopicSpec,
    PartitionMap as _PartitionMap,
    CommonCreateRequest as _CommonCreateRequest,
    MetadataTopicSpec as _MetadataTopicSpec,
    WatchTopicStream as _WatchTopicStream,
    MetaUpdateTopicSpec as _MetaUpdateTopicSpec,
    MessageMetadataTopicSpec as _MessageMetadataTopicSpec,
    SmartModuleSpec as _SmartModuleSpec,
    MetadataSmartModuleSpec as _MetadataSmartModuleSpec,
    WatchSmartModuleStream as _WatchSmartModuleStream,
    MessageMetadataSmartModuleSpec as _MessageMetadataSmartModuleSpec,
    MetaUpdateSmartModuleSpec as _MetaUpdateSmartModuleSpec,
    MetadataPartitionSpec as _MetadataPartitionSpec,
)
from enum import Enum
from ._fluvio_python import Error as FluviorError  # noqa: F401
import typing


class Record:
    """The individual record for a given stream."""

    _inner: _Record

    def __init__(self, inner: _Record):
        self._inner = inner

    def offset(self) -> int:
        """The offset from the initial offset for a given stream."""
        return self._inner.offset()

    def value(self) -> typing.List[int]:
        """Returns the contents of this Record's value"""
        return self._inner.value()

    def value_string(self) -> str:
        """The UTF-8 decoded value for this record."""
        return self._inner.value_string()

    def key(self) -> typing.List[int]:
        """Returns the contents of this Record's key, if it exists"""
        return self._inner.key()

    def key_string(self) -> str:
        """The UTF-8 decoded key for this record."""
        return self._inner.key_string()

    def timestamp(self) -> int:
        """Timestamp of this record."""
        return self._inner.timestamp()


class RecordMetadata:
    """Metadata of a record send to a topic."""

    _inner: _RecordMetadata

    def __init__(self, inner: _RecordMetadata):
        self._inner = inner

    def offset(self) -> int:
        """Return the offset of the sent record in the topic/partition."""
        return self._inner.offset()

    def partition_id(self) -> int:
        """Return the partition index the record was sent to."""
        return self._inner.partition_id()


class ProduceOutput:
    """Returned by of `TopicProducer.send` call allowing access to sent record metadata."""

    _inner: _ProduceOutput

    def __init__(self, inner: _ProduceOutput) -> None:
        self._inner = inner

    def wait(self) -> typing.Optional[RecordMetadata]:
        """Wait for the record metadata.

        This is a blocking call and may only return a `RecordMetadata` once.
        Any subsequent call to `wait` will return a `None` value.
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
        res = await self._inner.async_wait()
        if res is None:
            return None
        return RecordMetadata(res)


class Offset:
    """Describes the location of an event stored in a Fluvio partition."""

    _inner: _Offset

    @classmethod
    def absolute(cls, index: int):
        """Creates an absolute offset with the given index"""
        return cls(_Offset.absolute(index))

    @classmethod
    def beginning(cls):
        """Creates a relative offset starting at the beginning of the saved log"""
        return cls(_Offset.beginning())

    @classmethod
    def end(cls):
        """Creates a relative offset pointing to the newest log entry"""
        return cls(_Offset.end())

    @classmethod
    def from_beginning(cls, offset: int):
        """Creates a relative offset a fixed distance after the oldest log
        entry
        """
        return cls(_Offset.from_beginning(offset))

    @classmethod
    def from_end(cls, offset: int):
        """Creates a relative offset a fixed distance before the newest log
        entry
        """
        return cls(_Offset.from_end(offset))

    def __init__(self, inner: _Offset):
        self._inner = inner


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
        return self._generator(self._inner.stream(offset._inner))

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
        return self._async_generator(await self._inner.async_stream(offset._inner))

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
        return self._generator(
            self._inner.stream_with_config(offset._inner, config._inner)
        )

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
            async for i in await consumer.async_stream_with_config(Offset.beginning(), config):
                # do something with i

        Returns:
            `AsyncIterator[Record]`

        """
        return self._async_generator(
            await self._inner.async_stream_with_config(offset._inner, config._inner)
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
        return self._generator(self._inner.stream(offset._inner))

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
        return self._async_generator(await self._inner.async_stream(offset._inner))

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
            await self._inner.async_stream_with_config(offset._inner, config._inner)
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

    def topic_producer(self, topic: str) -> TopicProducer:
        """
        Creates a new `TopicProducer` for the given topic name.

        Currently, producers are scoped to a specific Fluvio topic. That means
        when you send events via a producer, you must specify which partition
        each event should go to.
        """
        return TopicProducer(self._inner.topic_producer(topic))


class PartitionMap:
    _inner: _PartitionMap

    def __init__(self, inner: _PartitionMap):
        self._inner = inner

    @classmethod
    def new(cls, partition: int, replicas: typing.List[int]):
        return cls(_PartitionMap.new(partition, replicas))


class TopicSpec:
    _inner: _TopicSpec

    def __init__(self, inner: _TopicSpec):
        self._inner = inner

    @classmethod
    def new_assigned(cls, partition_maps: typing.List[PartitionMap]):
        partition_maps = [x._inner for x in partition_maps]
        return cls(_TopicSpec.new_computed(partition_maps))

    @classmethod
    def new_computed(cls, partitions: int, replication: int, ignore: bool):
        return cls(_TopicSpec.new_computed(partitions, replication, ignore))


class CommonCreateRequest:
    _inner: _CommonCreateRequest

    def __init__(self, inner: _CommonCreateRequest):
        self._inner = inner

    @classmethod
    def new(cls, name: str, dry_run: bool, timeout: int):
        return cls(_CommonCreateRequest.new(name, dry_run, timeout))


class MetadataTopicSpec:
    _inner: _MetadataTopicSpec

    def __init__(self, inner: _MetadataTopicSpec):
        self._inner = inner

    def name(self) -> str:
        return self._inner.name()


class MessageMetadataTopicSpec:
    _inner: _MessageMetadataTopicSpec

    def __init__(self, inner: _MessageMetadataTopicSpec):
        self._inner = inner

    def is_update(self) -> bool:
        return self._inner.is_update()

    def is_delete(self) -> bool:
        return self._inner.is_delete()

    def metadata_topic_spec(self) -> MetadataTopicSpec:
        return MetadataTopicSpec(self._inner.metadata_topic_spec())


class MetaUpdateTopicSpec:
    _inner: _MetaUpdateTopicSpec

    def __init__(self, inner: _MetaUpdateTopicSpec):
        self._inner = inner

    def all(self) -> typing.List[MetadataTopicSpec]:
        inners = self._inner.all()
        return [MetadataTopicSpec(i) for i in inners]

    def changes(self) -> typing.List[MessageMetadataTopicSpec]:
        inners = self._inner.changes()
        return [MessageMetadataTopicSpec(i) for i in inners]

    def epoch(self) -> int:
        return self._inner.epoch()


class SmartModuleSpec:
    _inner: _SmartModuleSpec

    def __init__(self, inner: _SmartModuleSpec):
        self._inner = inner

    @classmethod
    def new(cls, path: str):
        f = open(path, mode="rb")
        data = f.read()
        f.close()
        return cls(_SmartModuleSpec.with_binary(data))


class MetadataSmartModuleSpec:
    _inner: _MetadataSmartModuleSpec

    def __init__(self, inner: _MetadataSmartModuleSpec):
        self._inner = inner

    def name(self) -> str:
        return self._inner.name()


class MessageMetadataSmartModuleSpec:
    _inner: _MessageMetadataSmartModuleSpec

    def __init__(self, inner: _MessageMetadataSmartModuleSpec):
        self._inner = inner

    def is_update(self) -> bool:
        return self._inner.is_update()

    def is_delete(self) -> bool:
        return self._inner.is_delete()

    def metadata_smart_module_spec(self) -> MetadataSmartModuleSpec:
        return MetadataSmartModuleSpec(self._inner.metadata_smart_module_spec())


class MetaUpdateSmartModuleSpec:
    _inner: _MetaUpdateSmartModuleSpec

    def __init__(self, inner: _MetaUpdateSmartModuleSpec):
        self._inner = inner

    def all(self) -> typing.List[MetadataSmartModuleSpec]:
        inners = self._inner.all()
        return [MetadataSmartModuleSpec(i) for i in inners]

    def changes(self) -> typing.List[MessageMetadataSmartModuleSpec]:
        inners = self._inner.changes()
        return [MessageMetadataSmartModuleSpec(i) for i in inners]

    def epoch(self) -> int:
        return self._inner.epoch()


class MetadataPartitionSpec:
    _inner: _MetadataPartitionSpec

    def __init__(self, inner: _MetadataPartitionSpec):
        self._inner = inner

    def name(self) -> str:
        return self._inner.name()


class FluvioAdmin:
    _inner: _FluvioAdmin

    def __init__(self, inner: _FluvioAdmin):
        self._inner = inner

    def connect():
        return FluvioAdmin(_FluvioAdmin.connect())

    def connect_with_config(config: FluvioConfig):
        return FluvioAdmin(_FluvioAdmin.connect_with_config(config._inner))

    def create_topic(self, topic: str):

        partitions = 1
        replication = 1
        ignore_rack = True
        spec = _TopicSpec.new_computed(partitions, replication, ignore_rack)
        dry_run = False
        return self._inner.create_topic(topic, dry_run, spec)

    def create_topic_spec(self, topic: str, dry_run: bool, spec: TopicSpec):
        return self._inner.create_topic(topic, dry_run, spec._inner)

    def create_topic_with_config(
        self, topic: str, req: CommonCreateRequest, spec: TopicSpec
    ):
        return self._inner.create_topic_with_config(topic, req._inner, spec._inner)

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
