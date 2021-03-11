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
            producer.send_record("FOOBAR %s " % i, 0)

    def test_consume_with_iterator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer('my-topic-iterator')
        for i in range(10):
            producer.send_record("FOOBAR %s " % i, 0)

        consumer = fluvio.partition_consumer('my-topic-iterator', 0)
        count = 0
        for i in consumer.stream(0):
            print("THIS IS IN AN ITERATOR! %s" % i)
            count += 1
            if count >= 10:
                break
