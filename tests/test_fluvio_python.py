from fluvio import Fluvio
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

    def test_consume_with_iterator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer('my-topic-iterator')
        for i in range(10):
            producer.send_record_string("record-%s" % i, 0)

        consumer = fluvio.partition_consumer('my-topic-iterator', 0)
        count = 0
        for i in consumer.stream(0):
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
        for i in consumer.stream(0):
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
