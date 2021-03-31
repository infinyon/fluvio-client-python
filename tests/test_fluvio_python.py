from fluvio import (Fluvio, FluviorError, Offset)
import unittest


class TestFluvioMethods(unittest.TestCase):
    def test_connect(self):
        # A very simple test
        Fluvio.connect()

    def test_produce(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer('my-topic-produce')
        for i in range(10):
            producer.send_record_string("FOOBAR %s " % i, 0)

    def test_produce_on_uncreated_topic(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer('a-topic-that-does-not-exist')
        try:
            producer.send_record_string("THIS SHOULD FAIL", 0)
        except FluviorError as e:

            self.assertEqual(
                e.args,
                ('Partition not found: a-topic-that-does-not-exist-0',)
            )
            print('ERROR: %s' % e)

    def test_consume_with_iterator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer('my-topic-iterator')
        for i in range(10):
            producer.send_record_string("record-%s" % i, 0)

        consumer = fluvio.partition_consumer('my-topic-iterator', 0)
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
        producer = fluvio.topic_producer('my-topic-key-value-iterator')
        for i in range(10):
            producer.send("foo".encode(), ("record-%s" % i).encode())

        consumer = fluvio.partition_consumer('my-topic-key-value-iterator', 0)
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
        producer = fluvio.topic_producer('my-topic-batch-producer')

        records = []
        for i in range(10):
            record = (("%s" % i).encode(), ("record-%s" % i).encode())
            records.append(record)

        producer.send_all(records)

        consumer = fluvio.partition_consumer('my-topic-batch-producer', 0)
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
