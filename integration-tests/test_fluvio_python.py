from string import ascii_lowercase
from fluvio import Fluvio, Offset, ConsumerConfig, SmartModuleKind, FluvioConfig
from fluvio import FluvioAdmin, TopicSpec
import unittest
import uuid
import os
import itertools
import time


def create_smartmodule(sm_name, sm_path):
    # Normally it would be this code, but bare wasm smartmodules
    # are used in the python client testing, & only the cli supports it so far
    # dry_run = False
    # fluvio_admin = FluvioAdmin.connect()
    # fluvio_admin.create_smartmodule(sm_name, sm_path, dry_run)
    import subprocess

    subprocess.run(
        f"fluvio smartmodule create {sm_name} --wasm-file {sm_path}", shell=True
    ).check_returncode()


class CommonFluvioSmartModuleTestCase(unittest.TestCase):
    def common_setup(self, sm_path=None):
        self.admin = FluvioAdmin.connect()
        self.topic = str(uuid.uuid4())
        self.sm_name = str(uuid.uuid4())
        self.sm_path = sm_path

        if sm_path is not None:
            create_smartmodule(self.sm_name, sm_path)

            # FIXME: without this the tests fail. Some topics get created but with offset -1
            time.sleep(2)

        try:
            self.admin.create_topic(self.topic)
        except Exception as err:
            print("Retrying after create_topic error {}", err)
            time.sleep(5)
            self.admin.create_topic(self.topic)

    def setUp(self):
        self.common_setup()

    def tearDown(self):
        self.admin.delete_topic(self.topic)
        time.sleep(1)
        if self.sm_path is not None:
            self.admin.delete_smartmodule(self.sm_name)


class CommonAsyncFluvioSmartModuleTestCase(unittest.IsolatedAsyncioTestCase):
    def common_setup(self, sm_path=None):
        self.admin = FluvioAdmin.connect()
        self.topic = str(uuid.uuid4())
        self.sm_name = str(uuid.uuid4())
        self.sm_path = sm_path

        if sm_path is not None:
            create_smartmodule(self.sm_name, sm_path)

        try:
            self.admin.create_topic(self.topic)
        except Exception as err:
            print("Retrying after create_topic error {}", err)
            time.sleep(5)
            self.admin.create_topic(self.topic)

        # FIXME: without this the tests fail. Some topics get created but with offset -1
        time.sleep(2)

    def setUp(self):
        self.common_setup()

    def tearDown(self):
        self.admin.delete_topic(self.topic)
        time.sleep(1)
        if self.sm_path is not None:
            self.admin.delete_smartmodule(self.sm_name)


class TestFluvioFilterSmartModules(CommonFluvioSmartModuleTestCase):
    def setUp(self):
        # Smartmodule source:
        sm_path = os.path.abspath("smartmodules-for-ci/smartmodule_filter_on_a.wasm")
        self.common_setup(sm_path)

    def test_consume_with_smartmodule_by_name(self):
        """
        Test adds a the alphabet into a topic in the format of record-[letter]

        A wasm smart module is added to the filter and a all messages are
        retrieved and stored in the records list We can then assert the
        following:

        - There should be 1 item
        - It should be record-a

        """

        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in list(ascii_lowercase)[::-1]:
            producer.send_string(f"record-{i}")
        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        records.append(
            bytearray(
                next(consumer.stream_with_config(Offset.beginning(), config)).value()
            ).decode()
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], "record-a")

    def test_consumer_config_no_name(self):
        fluvio = Fluvio.connect()
        consumer = fluvio.partition_consumer(self.topic, 0)
        config = ConsumerConfig()
        config.smartmodule(name="does_not_exist")

        with self.assertRaises(Exception) as ctx:
            next(consumer.stream_with_config(Offset.beginning(), config))
        self.assertEqual(
            ctx.exception.args, ("SmartModule does_not_exist was not found",)
        )


class TestFluvioFilterSmartModulesWithParams(CommonFluvioSmartModuleTestCase):
    def setUp(self):
        # Source at:
        # https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/filter_with_param/src/lib.rs#L1-L24
        sm_path = os.path.abspath(
            "smartmodules-for-ci/fluvio_smartmodule_filter_param.wasm"
        )
        self.common_setup(sm_path)

    def test_consume_with_filter_params_example_with_params(self):
        """
        This smartmodule is from https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/filter_with_param/src/lib.rs#L1-L24  # noqa: E501
        """
        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name, params={"key": "o"})

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in ["foo", "bar", "fooquux", "baz"]:
            producer.send_string(f"{i}")

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(2):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "foo")
        self.assertEqual(records[1], "fooquux")

    def test_consume_with_filter_params_example_without_params(self):
        """
        This smartmodule is from https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/filter_with_param/src/lib.rs#L1-L24  # noqa: E501
        """
        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in [
            "foo",
            "bar",
            "fooquux",
            "baz",
        ]:
            producer.send_string(f"{i}")

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(2):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "bar")
        self.assertEqual(records[1], "baz")


class TestFluvioAggregateSmartModules(CommonFluvioSmartModuleTestCase):
    def setUp(self):
        # Smartmodule source:
        # https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/aggregate/src/lib.rs#L1-L13
        sm_path = os.path.abspath(
            "smartmodules-for-ci/fluvio_smartmodule_aggregate.wasm"
        )
        self.common_setup(sm_path)

    def test_consume_with_aggregate_example_without_kind_param(self):
        """
        This smartmodule is from https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/aggregate/src/lib.rs#L1-L13  # noqa: E501
        """
        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name, aggregate="1".encode())

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in ["a", "b", "c", "d"]:
            producer.send_string(f"{i}")

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(4):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 4)

        # TODO: These should all be prefixed by "1".
        self.assertEqual(records[0], "1a")
        self.assertEqual(records[1], "1ab")
        self.assertEqual(records[2], "1abc")
        self.assertEqual(records[3], "1abcd")

    def test_consume_with_aggregate_example_with_kind_param(self):
        """
        This smartmodule is from https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/aggregate/src/lib.rs#L1-L13  # noqa: E501
        """
        config = ConsumerConfig()
        config.smartmodule(
            name=self.sm_name, aggregate="1".encode(), kind=SmartModuleKind.Aggregate
        )

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in ["a", "b", "c", "d"]:
            producer.send_string(f"{i}")

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(4):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 4)

        # TODO: These should all be prefixed by "1".
        self.assertEqual(records[0], "1a")
        self.assertEqual(records[1], "1ab")
        self.assertEqual(records[2], "1abc")
        self.assertEqual(records[3], "1abcd")

    def test_consume_with_aggregate_example_without_initial_aggregate(self):
        """
        This smartmodule is from https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/aggregate/src/lib.rs#L1-L13  # noqa: E501
        """
        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in ["a", "b", "c", "d"]:
            producer.send_string(f"{i}")

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(4):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 4)

        # TODO: These should all **NOT** be prefixed by "1".
        self.assertEqual(records[0], "a")
        self.assertEqual(records[1], "ab")
        self.assertEqual(records[2], "abc")
        self.assertEqual(records[3], "abcd")


class TestFluvioMapSmartModules(CommonFluvioSmartModuleTestCase):
    def setUp(self):
        # Smartmodule source:
        # https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/map/src/lib.rs#L1-L10
        sm_path = os.path.abspath("smartmodules-for-ci/fluvio_smartmodule_map.wasm")
        self.common_setup(sm_path)

    def test_consume_with_aggregate_example(self):
        """
        This smartmodule is from https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/map/src/lib.rs#L1-L10  # noqa: E501
        """
        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in ["foo", "bar"]:
            producer.send_string(f"{i}")

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(2):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], "FOO")
        self.assertEqual(records[1], "BAR")


class TestFluvioFilterMapSmartModules(CommonFluvioSmartModuleTestCase):
    def setUp(self):
        # Smartmodule source:
        # https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/filter_map/src/lib.rs#L1-L17
        sm_path = os.path.abspath(
            "smartmodules-for-ci/fluvio_smartmodule_filter_map.wasm"
        )
        self.common_setup(sm_path)

    def test_consume_with_filter_map_example(self):
        """
        This smartmodule is from
        https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/filter_map/src/lib.rs#L1-L17  # noqa: E501
        This SmartModule filters out all odd numbers, and divides all even
        numbers by 2.

        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] -> [1, 2, 3, 4, 5]
        """
        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in range(1, 11):
            producer.send_string(f"{i}")

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(5):
            records.append(bytearray(next(stream).value()).decode())
        self.assertEqual(len(records), 5)
        self.assertEqual(records[0], "1")
        self.assertEqual(records[1], "2")
        self.assertEqual(records[2], "3")
        self.assertEqual(records[3], "4")
        self.assertEqual(records[4], "5")


class TestFluvioArrayMapSmartModules(CommonFluvioSmartModuleTestCase):
    def setUp(self):
        # Smartmodule source:
        # https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/array_map_json_array/src/lib.rs#L1-L55
        sm_path = os.path.abspath(
            "smartmodules-for-ci/fluvio_smartmodule_array_map_array.wasm"
        )
        self.common_setup(sm_path)

    def test_consume_with_array_map_example(self):
        """
        This smartmodule is from
        https://github.com/infinyon/fluvio/blob/2eacd6fc08e4c11aa5de738bfa178d4fb56c7f72/smartmodule/examples/array_map_json_array/src/lib.rs#L1-L55  # noqa: E501
        This SmartModule turns the record "["Apple", "Banana", "Cranberry"]"
        into three records of:
        "Apple"
        "Banana"
        "Cranberry"
        """
        config = ConsumerConfig()
        config.smartmodule(name=self.sm_name)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        producer.send_string('["Apple", "Banana", "Cranberry"]')

        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        stream = consumer.stream_with_config(Offset.beginning(), config)
        for i in range(3):
            records.append(bytearray(next(stream).value()).decode())

        self.assertEqual(len(records), 3)
        self.assertEqual(records[0], '"Apple"')
        self.assertEqual(records[1], '"Banana"')
        self.assertEqual(records[2], '"Cranberry"')


class TestAsyncFluvioMethods(CommonAsyncFluvioSmartModuleTestCase):
    async def test_consumer_with_interator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            await producer.async_send_string("record-%s" % i)

        consumer = fluvio.partition_consumer(self.topic, 0)
        astream = await consumer.async_stream(Offset.beginning())

        count = 0
        async for i in astream:
            self.assertEqual(bytearray(i.value()).decode(), "record-%s" % count)
            self.assertEqual(i.value_string(), "record-%s" % count)
            count += 1
            if count == 10:
                break

    async def test_multi_partition_consumer_with_interator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            await producer.async_send_string("record-%s" % i)

        consumer = fluvio.multi_partition_consumer(self.topic)
        astream = await consumer.async_stream(Offset.beginning())

        count = 0
        async for i in astream:
            self.assertEqual(bytearray(i.value()).decode(), "record-%s" % count)
            self.assertEqual(i.value_string(), "record-%s" % count)
            count += 1
            if count == 10:
                break

    async def test_multi_partition_multi_topic_consumer_with_interator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            await producer.async_send_string("record-%s" % i)

        consumer = fluvio.multi_topic_partition_consumer([(self.topic, 0)])
        astream = await consumer.async_stream(Offset.beginning())

        count = 0
        async for i in astream:
            self.assertEqual(bytearray(i.value()).decode(), "record-%s" % count)
            self.assertEqual(i.value_string(), "record-%s" % count)
            count += 1
            if count == 10:
                break


class TestFluvioMethods(CommonFluvioSmartModuleTestCase):
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

    def test_consume_with_smart_module_by_file_path(self):
        """
        Test adds a the alphabet into a topic in the format of record-[letter]

        A wasm smart module is added to the filter and a all messages are
        retrieved and stored in the records list We can then assert the
        following:

        - There should be 1 item
        - It should be record-a

        """

        sm_path = os.path.abspath("smartmodules-for-ci/smartmodule_filter_on_a.wasm")
        config = ConsumerConfig()
        config.smartmodule(path=sm_path, kind=SmartModuleKind.Filter)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in list(ascii_lowercase)[::-1]:
            producer.send_string(f"record-{i}")
        records = []

        consumer = fluvio.partition_consumer(self.topic, 0)
        records.append(
            bytearray(
                next(consumer.stream_with_config(Offset.beginning(), config)).value()
            ).decode()
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], "record-a")

    def test_consumer_with_interator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            producer.send_string("record-%s" % i)

        consumer = fluvio.partition_consumer(self.topic, 0)
        for count, i in enumerate(
            itertools.islice(consumer.stream(Offset.beginning()), 10)
        ):
            self.assertEqual(bytearray(i.value()).decode(), "record-%s" % count)
            self.assertEqual(i.value_string(), "record-%s" % count)

    def test_consumer_with_interator_generator(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            producer.send_string("%s" % i)

        consumer = fluvio.partition_consumer(self.topic, 0)

        stream = (
            int(i.value_string()) * 2
            for i in itertools.islice(consumer.stream(Offset.beginning()), 10)
        )

        for count, i in enumerate(stream):
            self.assertEqual(i, count * 2)

    def test_key_value(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            producer.send("foo".encode(), ("record-%s" % i).encode())

        consumer = fluvio.partition_consumer(self.topic, 0)
        for count, i in enumerate(
            itertools.islice(consumer.stream(Offset.beginning()), 10)
        ):
            self.assertEqual(bytearray(i.value()).decode(), "record-%s" % count)
            self.assertEqual(i.value_string(), "record-%s" % count)
            self.assertEqual(i.key_string(), "foo")
            self.assertEqual(i.key(), list("foo".encode()))

    def test_record_timestamp(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        producer.send_string("some_record")
        producer.flush()
        consumer = fluvio.partition_consumer(self.topic, 0)
        record = next(consumer.stream(Offset.beginning()))
        self.assertGreaterEqual(record.timestamp(), -1)

    def test_batch_produce(self):
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)

        records = []
        for i in range(10):
            record = (("%s" % i).encode(), ("record-%s" % i).encode())
            records.append(record)

        producer.send_all(records)

        consumer = fluvio.partition_consumer(self.topic, 0)
        for count, i in enumerate(
            itertools.islice(consumer.stream(Offset.beginning()), 10)
        ):
            self.assertEqual(bytearray(i.value()).decode(), "record-%s" % count)
            self.assertEqual(i.value_string(), "record-%s" % count)
            self.assertEqual(i.key_string(), ("%s" % count))
            self.assertEqual(i.key(), list(("%s" % count).encode()))


class TestFluvioErrors(CommonFluvioSmartModuleTestCase):
    def test_produce_on_uncreated_topic(self):
        fluvio = Fluvio.connect()
        topic_name = "not-created-topic"

        with self.assertRaises(Exception) as ctx:
            fluvio.topic_producer(topic_name)

        self.assertIn(
            "Topic not found: %s" % topic_name,
            ctx.exception.args[0],  # noqa: E501
        )

    def test_consumer_config_no_sm_name_or_path(self):
        config = ConsumerConfig()
        with self.assertRaises(Exception) as ctx:
            config.smartmodule()
        self.assertEqual(
            ctx.exception.args, ("Require either a path or a name for a smartmodule.",)
        )

    def test_consumer_config_no_sm_name(self):
        config = ConsumerConfig()
        with self.assertRaises(Exception) as ctx:
            config.smartmodule(name="foo", path="bar")

        self.assertEqual(
            ctx.exception.args, ("Only specify one of path or name not both.",)
        )

    def test_consumer_config_attribute_error(self):
        config = ConsumerConfig()
        with self.assertRaises(AttributeError) as ctx:
            config.smartmodule(kind="foobar")
        self.assertIn("'str' object has no attribute 'value'", ctx.exception.args[0])

    def test_consumer_config_no_file(self):
        config = ConsumerConfig()
        with self.assertRaises(Exception) as ctx:
            config.smartmodule(path="does_not_exist.wasm", kind=SmartModuleKind.Filter)
        self.assertIn(
            "No such file or directory (os error 2)",
            ctx.exception.args[0],
        )


class TestFluvioProduceFlush(CommonFluvioSmartModuleTestCase):
    def test_produce_flush(self):
        expected_output = ["record-%s" % i for i in range(10)]
        # Hopefully when the fluvio/producer variable goes out of scope,
        # garbage is collected
        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in expected_output:
            producer.send("".encode(), i.encode())

        producer.flush()

        import subprocess

        result = subprocess.run(
            "fluvio consume %s -B -d" % self.topic,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # The CLI appends an extra newline to the output.
        expected_output.append("")
        expected_output = "\n".join(expected_output)
        stdout = result.stdout

        self.assertEqual(expected_output, stdout)


class CommonFluvioAdminTestCase(unittest.TestCase):
    def common_setup(self):
        self.topic = str(uuid.uuid4())
        self.sm_name = str(uuid.uuid4())
        self.sm_path = os.path.abspath(
            "smartmodules-for-ci/smartmodule_filter_on_a.wasm"
        )

    def setUp(self):
        self.common_setup()


class TestFluvioAdminTopic(CommonFluvioAdminTestCase):
    def setUp(self):
        self.common_setup()
        fluvio_admin = FluvioAdmin.connect()
        all_partitions = fluvio_admin.list_partitions([])
        self.num_partitions_start = len(all_partitions)

    def test_admin_topic(self):
        fluvio_admin = FluvioAdmin.connect()
        topic_spec = TopicSpec.new_computed(3, 1, False)

        # create topic
        fluvio_admin.create_topic_spec(self.topic, False, topic_spec)

        # watch topic
        stream = fluvio_admin.watch_topic()

        all_topics = next(stream).all()
        all_topic_names = [topic.name() for topic in all_topics]
        self.assertIn(self.topic, all_topic_names)

        # list all topics
        topic_specs = fluvio_admin.all_topics()
        names = [i.name() for i in topic_specs]
        self.assertIn(self.topic, names)

        # list topics
        topic_specs = fluvio_admin.list_topics([self.topic])
        self.assertEqual(topic_specs[0].name(), self.topic)

        # list topic summary
        topic_specs = fluvio_admin.list_topics_with_params([self.topic], True)
        self.assertEqual(topic_specs[0].name(), self.topic)

        # delete topic
        fluvio_admin.delete_topic(self.topic)

        # list all topics
        topic_specs = fluvio_admin.all_topics()
        names = [i.name() for i in topic_specs]
        self.assertNotIn(self.topic, names)

        # list topics
        topic_specs = fluvio_admin.list_topics([self.topic])
        self.assertEqual(len(topic_specs), 0)

    def test_admin_smart_module(self):
        fluvio_admin = FluvioAdmin.connect()

        # create smart module
        fluvio_admin.create_smartmodule(self.sm_name, self.sm_path, False)

        # watch smart module
        stream = fluvio_admin.watch_smartmodule()
        all_smart_modules = next(stream).all()
        all_smart_module_names = [sm.name() for sm in all_smart_modules]
        self.assertIn(self.sm_name, all_smart_module_names)

        # list smart modules
        smart_modules = fluvio_admin.list_smartmodules([self.sm_name])
        self.assertEqual(smart_modules[0].name(), self.sm_name)

        # delete smart module
        fluvio_admin.delete_smartmodule(self.sm_name)

        # list smart modules
        smart_modules = fluvio_admin.list_smartmodules([self.sm_name])
        self.assertEqual(len(smart_modules), 0)

    def test_admin_paritions(self):
        fluvio_admin = FluvioAdmin.connect()

        topic = str(uuid.uuid4())
        try:
            fluvio_admin.create_topic(topic)
        except Exception as err:
            print("Retrying after create_topic error {}", err)
            time.sleep(5)
            fluvio_admin.create_topic(topic)

        # list partitions
        all_partitions = fluvio_admin.list_partitions([])
        print(all_partitions)
        self.assertNotEqual(len(all_partitions), 0)

        fluvio_admin.delete_topic(topic)
