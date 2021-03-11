from ._fluvio_python import (
    Fluvio as _Fluvio,
    PartitionConsumer as _PartitionConsumer,
    PartitionConsumerStream as _PartitionConsumerStream,
    TopicProducer as _TopicProducer,
)


class PartitionConsumerStream:
    def __init__(self, inner: _PartitionConsumerStream):
        self.inner = inner

    def __iter__(self):
        return self

    def __next__(self):
        return self.inner.next()


class PartitionConsumer:
    def __init__(self, inner: _PartitionConsumer):
        self.inner = inner

    def stream(self, offset: int) -> PartitionConsumerStream:
        return PartitionConsumerStream(self.inner.stream(offset))


class TopicProducer:
    def __init__(self, inner: _TopicProducer):
        self.inner = inner

    def send_record(self, record: str, partition: int) -> None:
        self.inner.send_record(record, partition)


class Fluvio:
    ''' The base connector to
    '''
    def __init__(self, inner: _Fluvio):
        self.inner = inner

    @classmethod
    def connect(cls):
        '''Connect to the fluvio cluster.
        '''
        return cls(_Fluvio.connect())

    def partition_consumer(
        self,
        topic: str,
        partition: int
    ) -> PartitionConsumer:
        '''Get the `PartitionConsumer` for a `topic` and a `partition`.
        '''
        return PartitionConsumer(
            self.inner.partition_consumer(topic, partition)
        )

    def topic_producer(self, topic: str) -> TopicProducer:
        '''Get the `TopicProducer` for `topic`.
        '''
        return self.inner.topic_producer(topic)
