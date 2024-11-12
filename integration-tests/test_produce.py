import unittest
import uuid
import itertools
import time

from fluvio import Fluvio, Offset, FluvioConfig
from fluvio import FluvioAdmin
from test_base import CommonFluvioSetup

class TestFluvioProduce(CommonFluvioSetup):
    def test_connect(self):
        # A very simple test
        Fluvio.connect()

    def test_connect_with_config(self):
        config = FluvioConfig.load()
        Fluvio.connect_with_config(config)

    def test_produce(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            producer.send_string("FOOBAR %s " % i)

    def test_produce_record_metadata(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer(self.topic)

        msg_strings = ["Foobar %s" % i for i in range(10)]
        produce_outputs = [producer.send_string(msg) for msg in msg_strings]

        records = [
            (("%s" % i).encode(), msg_string.encode())
            for i, msg_string in enumerate(msg_strings)
        ]
        produce_outputs.extend(producer.send_all(records))

        record_metadata = [produce_output.wait() for produce_output in produce_outputs]

        partition_id = 0
        consumer = fluvio.partition_consumer(self.topic, partition_id)
        messages = list(
            itertools.islice(consumer.stream(Offset.beginning()), len(produce_outputs))
        )

        for metadata, msg in zip(record_metadata, messages):
            self.assertNotEqual(metadata, None)
            self.assertEqual(metadata.partition_id(), partition_id)
            self.assertEqual(metadata.offset(), msg.offset())

        for produce_output in produce_outputs:
            # subsequent calls to po.wait shall return None
            self.assertEqual(produce_output.wait(), None)


class TestFluvioProduceAsync(unittest.IsolatedAsyncioTestCase, CommonFluvioSetup):
    async def test_async_produce(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            ret = await producer.async_send_string("FOOBAR %s " % i)
            ret.wait()

    async def test_async_produce_async_wait(self):
        """simple test, test_async_produce_record_metadata is more comprehensive"""
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer(self.topic)
        future = await producer.async_send_string("FOOBAR async async")
        out_future = future.async_wait()
        result = await out_future
        self.assertNotEqual(result, None)

    async def test_async_produce_record_metadata(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer(self.topic)

        msg_strings = ["Foobar %s" % i for i in range(10)]
        produce_outputs = [await producer.async_send_string(msg) for msg in msg_strings]

        records = [
            (("%s" % i).encode(), msg_string.encode())
            for i, msg_string in enumerate(msg_strings)
        ]
        produce_outputs.extend(await producer.async_send_all(records))

        record_metadata = [
            await produce_output.async_wait() for produce_output in produce_outputs
        ]

        partition_id = 0
        consumer = fluvio.partition_consumer(self.topic, partition_id)
        messages = list(
            itertools.islice(consumer.stream(Offset.beginning()), len(produce_outputs))
        )

        for metadata, msg in zip(record_metadata, messages):
            self.assertNotEqual(metadata, None)
            self.assertEqual(metadata.partition_id(), partition_id)
            self.assertEqual(metadata.offset(), msg.offset())

        for produce_output in produce_outputs:
            # subsequent calls to po.wait shall return None
            self.assertEqual(produce_output.wait(), None)
