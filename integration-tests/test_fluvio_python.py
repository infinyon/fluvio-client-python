from fluvio import (Fluvio, FluviorError, Offset)
import unittest
import uuid


def create_topic(topic):
    import subprocess
    subprocess.run("fluvio topic create %s" % topic, shell=True)


def delete_topic(topic):
    import subprocess
    subprocess.run("fluvio topic delete %s" % topic, shell=True)

class TestFluvioMethods(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.topic = str(uuid.uuid4())
        create_topic(self.topic)

    def tearDown(self):
        delete_topic(self.topic)

    async def test_connect(self):
        # A very simple test
        await Fluvio.connect()

    async def test_produce(self):
        fluvio = await Fluvio.connect()

        producer = await fluvio.topic_producer(self.topic)
        for i in range(10):
            await producer.send_string("FOOBAR %s " % i)

    async def test_consume_with_iterator(self):
        fluvio = await Fluvio.connect()
        producer = await fluvio.topic_producer(self.topic)
        for i in range(10):
            await producer.send_string("record-%s" % i)

        consumer = await fluvio.partition_consumer(self.topic, 0)
        count = 0
        for i in await consumer.stream(Offset.beginning()):
            print("THIS IS IN AN ITERATOR! %s" % i.value())
            self.assertEqual(
                bytearray(i.value()).decode(), 'record-%s' % count
            )
            self.assertEqual(i.value_string(), 'record-%s' % count)
            count += 1
            if count >= 10:
                break

    async def test_key_value(self):
        fluvio = await Fluvio.connect()
        producer = await fluvio.topic_producer(self.topic)
        for i in range(10):
            await producer.send("foo".encode(), ("record-%s" % i).encode())

        consumer = await fluvio.partition_consumer(self.topic, 0)
        count = 0
        for i in await consumer.stream(Offset.beginning()):
            print(
                "THIS IS IN AN ITERATOR! key - %s, value - %s" % (
                    i.key(),
                    i.value()
                )
            )
            self.assertEqual(
                bytearray(i.value()).decode(), 'record-%s' % count
            )
            self.assertEqual(i.value_string(), 'record-%s' % count)
            self.assertEqual(i.key_string(), 'foo')
            self.assertEqual(i.key(), list('foo'.encode()))

            count += 1
            if count >= 10:
                break

    async def test_batch_produce(self):
        fluvio = await Fluvio.connect()
        producer = await fluvio.topic_producer(self.topic)

        records = []
        for i in range(10):
            record = (("%s" % i).encode(), ("record-%s" % i).encode())
            records.append(record)

        await producer.send_all(records)

        consumer = await fluvio.partition_consumer(self.topic, 0)
        count = 0
        for i in await consumer.stream(Offset.beginning()):
            self.assertEqual(
                bytearray(i.value()).decode(), 'record-%s' % count
            )
            self.assertEqual(i.value_string(), 'record-%s' % count)
            self.assertEqual(i.key_string(), ('%s' % count))
            self.assertEqual(i.key(), list(('%s' % count).encode()))

            count += 1
            if count >= 10:
                break


class TestFluvioErrors(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.topic = str(uuid.uuid4())

    async def test_produce_on_uncreated_topic(self):
        fluvio = await Fluvio.connect()

        error = None
        try:
            await fluvio.topic_producer(self.topic)
        except FluviorError as e:
            error = e
            print('ERROR: %s' % e)

        self.assertTrue(error is not None)
        self.assertEqual(
            error.args,
            (
                'Topic not found: %s' % self.topic,  # noqa: E501
            )
        )
