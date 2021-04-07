from fluvio import (Fluvio, FluviorError, Offset)
import unittest

import platform
def topic_prefix(topic_suffix):
    return (
        '%s-%s-%s-%s' % (
            platform.system().lower(),
            platform.python_version_tuple()[0],
            platform.python_version_tuple()[1],
            topic_suffix,
        )
    )

def create_topic(topic):
    import subprocess
    subprocess.run("fluvio topic create %s" % topic, shell=True)

def delete_topic(topic):
    import subprocess
    subprocess.run("fluvio topic delete %s" % topic, shell=True)

class TestFluvioMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for i in [
            'my-topic-produce',
            'my-topic-iterator',
            'my-topic-key-value-iterator',
            'my-topic-batch-producer'
         ]:
            create_topic(topic_prefix(i))

    @classmethod
    def tearDownClass(cls):
        for i in [
            'my-topic-produce',
            'my-topic-iterator',
            'my-topic-key-value-iterator',
            'my-topic-batch-producer'
        ]:
            delete_topic(topic_prefix(i))

    def test_connect(self):
        # A very simple test
        Fluvio.connect()

    def test_produce(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer(topic_prefix('my-topic-produce'))
        for i in range(10):
            producer.send_record_string("FOOBAR %s " % i, 0)

    def test_produce_on_uncreated_topic(self):
        fluvio = Fluvio.connect()
        topic = topic_prefix('a-topic-that-does-not-exist')

        producer = fluvio.topic_producer(topic)
        try:
            producer.send_record_string("THIS SHOULD FAIL", 0)
        except FluviorError as e:

            self.assertEqual(
                e.args,
                (
                    'Partition not found: %s-0' % topic,
                )
            )
            print('ERROR: %s' % e)

    def test_consume_with_iterator(self):
        fluvio = Fluvio.connect()
        topic = topic_prefix('my-topic-iterator')
        producer = fluvio.topic_producer(topic)
        for i in range(10):
            producer.send_record_string("record-%s" % i, 0)

        consumer = fluvio.partition_consumer(topic, 0)
        count = 0
        for i in consumer.stream(Offset.beginning()):
            print("THIS IS IN AN ITERATOR! %s" % i.value())
            self.assertEqual(
                bytearray(i.value()).decode(), 'record-%s' % count
            )
            self.assertEqual(i.value_string(), 'record-%s' % count)
            count += 1
            if count >= 10:
                break

    def test_key_value(self):
        fluvio = Fluvio.connect()
        topic = topic_prefix('my-topic-key-value-iterator')
        producer = fluvio.topic_producer(topic)
        for i in range(10):
            producer.send("foo".encode(), ("record-%s" % i).encode())

        consumer = fluvio.partition_consumer(topic, 0)
        count = 0
        for i in consumer.stream(Offset.beginning()):
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

    def test_batch_produce(self):
        fluvio = Fluvio.connect()
        topic = topic_prefix('my-topic-batch-producer')
        producer = fluvio.topic_producer(topic)

        records = []
        for i in range(10):
            record = (("%s" % i).encode(), ("record-%s" % i).encode())
            records.append(record)

        producer.send_all(records)

        consumer = fluvio.partition_consumer(topic, 0)
        count = 0
        for i in consumer.stream(Offset.beginning()):
            self.assertEqual(
                bytearray(i.value()).decode(), 'record-%s' % count
            )
            self.assertEqual(i.value_string(), 'record-%s' % count)
            self.assertEqual(i.key_string(), ('%s' % count))
            self.assertEqual(i.key(), list(('%s' % count).encode()))

            count += 1
            if count >= 10:
                break
