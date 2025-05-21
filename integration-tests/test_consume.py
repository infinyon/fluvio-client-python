from string import ascii_lowercase
import time

from fluvio import ConsumerConfig, Fluvio, Offset, OffsetManagementStrategy
from fluvio import ConsumerConfigExtBuilder
from test_base import CommonFluvioSetup


class TestFluvioConsumer(CommonFluvioSetup):
    def setUp(self):
        self.common_setup()
        fluvio = Fluvio.connect()

        # Generate a set of test data for the topic
        producer = fluvio.topic_producer(self.topic)
        for i in list(ascii_lowercase)[::-1]:
            producer.send_string(f"record-{i}")
        producer.flush()

    def test_consume_basic(self):
        """
        Basic consume fetch tests
        """
        fluvio = Fluvio.connect()

        # configure consumer
        builder = ConsumerConfigExtBuilder(self.topic)

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 2
        records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]

        # records = []
        # for i in range(1):
        #     records.append(bytearray(next(stream).value()).decode())

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "record-z")
        self.assertEqual(records[1], "record-y")

    def test_consume_at_offset_begin(self):
        """
        Basic consume fetch tests
        """
        fluvio = Fluvio.connect()

        # configure consumer to start from beginning (default)
        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.beginning())

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 2
        records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "record-z")
        self.assertEqual(records[1], "record-y")

    def test_consume_offset_managed_auto_manual(self):
        """
        Manual offset management test
        """
        fluvio = Fluvio.connect()

        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.beginning())
        builder.offset_strategy(OffsetManagementStrategy.MANUAL)
        builder.offset_consumer(self.consumer_id)

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 2
        records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]

        time.sleep(1)

        stream.offset_commit()
        stream.offset_flush()

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "record-z")
        self.assertEqual(records[1], "record-y")

        consumers = fluvio.consumer_offsets()
        consumer = next(
            (c for c in consumers if c.consumer_id == self.consumer_id), None
        )
        if consumer is None:
            self.fail("consumer not found")

        self.assertEqual(consumer.consumer_id, self.consumer_id)
        self.assertEqual(consumer.topic, self.topic)
        self.assertEqual(consumer.partition, 0)
        self.assertEqual(consumer.offset, 1)

    def test_consume_offset_managed_auto_commit_and_flush(self):
        """
        Automatic offset management test
        """
        fluvio = Fluvio.connect()

        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.beginning())
        builder.offset_strategy(OffsetManagementStrategy.AUTO)
        builder.offset_consumer(self.consumer_id)

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 2
        records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]

        stream.offset_commit()
        stream.offset_flush()

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "record-z")
        self.assertEqual(records[1], "record-y")

        consumers = fluvio.consumer_offsets()
        consumer = next(
            (c for c in consumers if c.consumer_id == self.consumer_id), None
        )
        if consumer is None:
            self.fail("consumer not found")

        self.assertEqual(consumer.consumer_id, self.consumer_id)
        self.assertEqual(consumer.topic, self.topic)
        self.assertEqual(consumer.partition, 0)
        self.assertEqual(consumer.offset, 1)

    def test_consume_offset_managed_auto_flush(self):
        """
         Consume with an offset_consume id defined

         This will case the cluster to track that offsets
         that consumer has consumed, stoping and resuming
         from last offset

         The equivalent of this test with the CLI is:
        `fluvio consume test-topic --offset-beginning --offset-consumer --end 1 foo`
         record-z


        `fluvio consume test-topic --offset-beginning --offset-consumer --end 2 foo`
         record-y

        """
        fluvio = Fluvio.connect()

        # configure consumer
        builder = ConsumerConfigExtBuilder(self.topic)
        builder.disable_continuous()
        builder.offset_consumer(self.consumer_id)
        builder.offset_start(Offset.beginning())
        builder.offset_strategy(OffsetManagementStrategy.AUTO)

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 2
        records = []
        for _ in range(num_items):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "record-z")
        self.assertEqual(records[1], "record-y")
        # ensure offset is flushed
        # with Auto, this normally happens with advancement of the stream and/or
        # closure of the client
        # but that isn't at a guaranteed point for python in the middle
        # of this test, so do this explictly
        stream.offset_flush()
        # connect again with a new client, but the same id
        fluvio = Fluvio.connect()

        # configure consumer
        # even though offset_start says to start at the beginning
        # it is expected start from the last offset consumed
        # because the offset_consumer is set
        builder = ConsumerConfigExtBuilder(self.topic)
        builder.disable_continuous()
        builder.offset_consumer(self.consumer_id)
        builder.offset_start(Offset.beginning())
        builder.offset_strategy(OffsetManagementStrategy.AUTO)

        stream = fluvio.consumer_with_config(config)

        num_items = 1
        records = []
        for _ in range(1):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], "record-x")

        consumers = fluvio.consumer_offsets()
        consumer = next(
            (c for c in consumers if c.consumer_id == self.consumer_id), None
        )
        if consumer is None:
            self.fail("consumer not found")

        self.assertEqual(consumer.consumer_id, self.consumer_id)
        self.assertEqual(consumer.topic, self.topic)
        self.assertEqual(consumer.partition, 0)
        self.assertEqual(consumer.offset, 1)

    def test_consume_at_offset_from_begin(self):
        fluvio = Fluvio.connect()

        # configure consumer to start 1 from beginning
        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.from_beginning(1))

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 1
        records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], "record-y")

    def test_consume_at_offset_from_end(self):
        """consume from offset::from_end(1)"""
        fluvio = Fluvio.connect()

        # configure consumer to start from end
        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.from_end(1))

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 1
        records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], "record-a")

    def test_consume_at_offset_absolute(self):
        """consume from offset::absolute(1)"""
        fluvio = Fluvio.connect()

        # configure consumer to start from end
        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.absolute(13))

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 1
        records = [bytearray(next(stream).value()).decode() for _ in range(num_items)]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], "record-m")

    def test_consume_disconnect(self):
        """Non-waiting fetch of two records from offset::from_end(1)

        Without the builder.disable_continuous() call, the consumer will
        hang waiting for the next record to be produced
        """
        fluvio = Fluvio.connect()

        # configure consumer to start from end
        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.from_end(1))
        builder.disable_continuous()

        config = builder.build()
        stream = fluvio.consumer_with_config(config)

        num_items = 2
        records = []
        for _ in range(num_items):
            try:
                record = bytearray(next(stream).value()).decode()
                records.append(record)
            except StopIteration:
                break
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], "record-a")

    def test_consume_deprecated(self):
        """
        Basic consume test with the older depcreated API

        This will be removed in the future
        """
        fluvio = Fluvio.connect()

        consumer = fluvio.partition_consumer(self.topic, 0)
        records = []

        config = ConsumerConfig()
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for _ in range(2):
            records.append(bytearray(next(stream).value()).decode())

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "record-z")
        self.assertEqual(records[1], "record-y")
