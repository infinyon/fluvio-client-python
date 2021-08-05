from ._fluvio_python import (
    Fluvio as _Fluvio,
    PartitionConsumer as _PartitionConsumer,
    PartitionConsumerStream as _PartitionConsumerStream,
    TopicProducer as _TopicProducer,
    ProducerBatchRecord as _ProducerBatchRecord,
    Record as _Record,
    Offset as _Offset
)
from ._fluvio_python import Error as FluviorError  # noqa: F401
import typing


class Record:
    '''The individual record for a given stream.
    '''
    _inner: _Record

    def __init__(self, inner: _Record):
        self._inner = inner

    def offset(self) -> int:
        '''The offset from the initial offset for a given stream.
        '''
        return self._inner.offset()

    def value(self) -> typing.List[int]:
        '''Returns the contents of this Record's value
        '''
        return self._inner.value()

    def value_string(self) -> str:
        '''The UTF-8 decoded value for this record.
        '''
        return self._inner.value_string()

    def key(self) -> typing.List[int]:
        '''Returns the contents of this Record's key, if it exists
        '''
        return self._inner.key()

    def key_string(self) -> str:
        '''The UTF-8 decoded key for this record.
        '''
        return self._inner.key_string()


class Offset:
    '''Describes the location of an event stored in a Fluvio partition.
    '''
    _inner: _Offset

    @classmethod
    def absolute(cls, index: int):
        '''Creates an absolute offset with the given index'''
        return cls(_Offset.absolute(index))

    @classmethod
    def beginning(cls):
        '''Creates a relative offset starting at the beginning of the saved log
        '''
        return cls(_Offset.beginning())

    @classmethod
    def end(cls):
        '''Creates a relative offset pointing to the newest log entry
        '''
        return cls(_Offset.end())

    @classmethod
    def from_beginning(cls, offset: int):
        '''Creates a relative offset a fixed distance after the oldest log
        entry
        '''
        return cls(_Offset.from_beginning(offset))

    @classmethod
    def from_end(cls, offset: int):
        '''Creates a relative offset a fixed distance before the newest log
        entry
        '''
        return cls(_Offset.from_end(offset))

    def __init__(self, inner: _Offset):
        self._inner = inner


class PartitionConsumerStream:
    '''An iterator for `PartitionConsumer.stream` method where each `__next__`
    returns a `Record`.

    Usage:

    ```python
    for i in consumer.stream(0):
        print(i.value())
        print(i.value_string())
    ```
    '''
    _inner: _PartitionConsumerStream

    def __init__(self, inner: _PartitionConsumerStream):
        self._inner = inner

    def __iter__(self):
        return self

    def __next__(self) -> typing.Optional[Record]:
        return Record(self._inner.next())


class PartitionConsumer:
    '''
    An interface for consuming events from a particular partition

    There are two ways to consume events: by "fetching" events and by
    "streaming" events. Fetching involves specifying a range of events that you
    want to consume via their Offset. A fetch is a sort of one-time batch
    operation: you’ll receive all of the events in your range all at once. When
    you consume events via Streaming, you specify a starting Offset and receive
    an object that will continuously yield new events as they arrive.
    '''

    _inner: _PartitionConsumer

    def __init__(self, inner: _PartitionConsumer):
        self._inner = inner

    def stream(self, offset: Offset) -> PartitionConsumerStream:
        '''
        Continuously streams events from a particular offset in the consumer’s
        partition. This returns a `PartitionConsumerStream` which is an
        iterator.

        Streaming is one of the two ways to consume events in Fluvio. It is a
        continuous request for new records arriving in a partition, beginning
        at a particular offset. You specify the starting point of the stream
        using an Offset and periodically receive events, either individually or
        in batches.
        '''
        return PartitionConsumerStream(self._inner.stream(offset._inner))


class TopicProducer:
    '''An interface for producing events to a particular topic.

    A `TopicProducer` allows you to send events to the specific topic it was
    initialized for. Once you have a `TopicProducer`, you can send events to
    the topic, choosing which partition each event should be delivered to.
    '''
    _inner: _TopicProducer

    def __init__(self, inner: _TopicProducer):
        self._inner = inner

    def send_string(self, buf: str) -> None:
        '''Sends a string to this producer’s topic
        '''
        return self.send([], buf.encode('utf-8'))

    def send(self, key: typing.List[int], value: typing.List[int]) -> None:
        '''
        Sends a key/value record to this producer's Topic.

        The partition that the record will be sent to is derived from the Key.
        '''
        return self._inner.send(key, value)

    def send_all(self, records: typing.List[typing.Tuple[bytes, bytes]]):
        '''
        Sends a list of key/value records as a batch to this producer's Topic.
        :param records: The list of records to send
        '''
        records_inner = [_ProducerBatchRecord(x, y) for (x, y) in records]
        return self._inner.send_all(records_inner)


class Fluvio:
    '''An interface for interacting with Fluvio streaming.'''
    _inner: _Fluvio

    def __init__(self, inner: _Fluvio):
        self._inner = inner

    @classmethod
    def connect(cls):
        '''Creates a new Fluvio client using the current profile from
        `~/.fluvio/config`

        If there is no current profile or the `~/.fluvio/config` file does not
        exist, then this will create a new profile with default settings and
        set it as current, then try to connect to the cluster using those
        settings.
        '''
        return cls(_Fluvio.connect())

    def partition_consumer(
        self,
        topic: str,
        partition: int
    ) -> PartitionConsumer:
        '''Creates a new `PartitionConsumer` for the given topic and partition

        Currently, consumers are scoped to both a specific Fluvio topic and to
        a particular partition within that topic. That means that if you have a
        topic with multiple partitions, then in order to receive all of the
        events in all of the partitions, you will need to create one consumer
        per partition.
        '''
        return PartitionConsumer(
            self._inner.partition_consumer(topic, partition)
        )

    def topic_producer(self, topic: str) -> TopicProducer:
        '''
        Creates a new `TopicProducer` for the given topic name.

        Currently, producers are scoped to a specific Fluvio topic. That means
        when you send events via a producer, you must specify which partition
        each event should go to.
        '''
        return TopicProducer(self._inner.topic_producer(topic))
