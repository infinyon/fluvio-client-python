from string import ascii_lowercase

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

    def test_consume_at_offset_begin_manual(self):
        """
        Manual offset management test
        """
        fluvio = Fluvio.connect()

        builder = ConsumerConfigExtBuilder(self.topic)
        builder.offset_start(Offset.beginning())
        builder.offset_strategy(OffsetManagementStrategy.MANUAL)
        builder.offset_consumer("test-consumer")

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
        self.assertEqual(len(consumers), 1)
        self.assertEqual(consumers[0].topic, self.topic)
        self.assertEqual(consumers[0].partition, 0)
        self.assertEqual(consumers[0].offset, 1)

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
        for i in range(2):
            records.append(bytearray(next(stream).value()).decode())

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "record-z")
        self.assertEqual(records[1], "record-y")
