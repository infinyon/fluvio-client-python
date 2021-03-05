from fluvio import Fluvio, PartitionConsumerStreamIterator
import unittest

class TestFluvioMethods(unittest.TestCase):
    def test_connect(self):
        fluvio = Fluvio.connect()

    def test_produce(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer('my-topic-produce')
        for i in range(10):
            producer.send_record("FOOBAR %s " % i, 0)

    def test_consume_with_while_loop(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer('my-topic-while')
        for i in range(10):
            producer.send_record("FOOBAR %s " % i, 0)

        consumer = fluvio.partition_consumer('my-topic-while', 0)
        stream = consumer.stream(0)
        count = 1
        curr = stream.next()
        while curr is not None and count < 10:
            print(curr)
            curr = stream.next()
            count += 1

    def test_consume_with_iterator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer('my-topic-iterator')
        for i in range(10):
            producer.send_record("FOOBAR %s " % i, 0)

        consumer = fluvio.partition_consumer('my-topic-iterator', 0)
        count = 0
        for i in PartitionConsumerStreamIterator(consumer.stream(0)):
            print("THIS IS IN AN ITERATOR! %s" % i)
            count += 1
            if count >= 10:
                break
