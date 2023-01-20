from string import ascii_lowercase
from fluvio import Fluvio, FluviorError, Offset, ConsumerConfig, SmartModuleKind
import unittest
import uuid
import os
import itertools


def create_topic(topic):
    import subprocess

    subprocess.run("fluvio topic create %s" % topic, shell=True)


def delete_topic(topic):
    import subprocess

    subprocess.run("fluvio topic delete %s" % topic, shell=True)


def create_smartmodule(sm_name, sm_path):
    import subprocess

    subprocess.run(
        "fluvio smartmodule create %s --wasm-file %s" % (
            sm_name, sm_path
        ), shell=True
    )


def delete_smartmodule(sm_name):
    import subprocess

    subprocess.run("fluvio smartmodule delete %s" % sm_name, shell=True)


class TestFluvioSmartModules(unittest.TestCase):
    def setUp(self):
        self.topic = str(uuid.uuid4())
        self.sm_name = str(uuid.uuid4())
        create_topic(self.topic)

        sm_path = os.path.abspath(
            "smartmodule_filter_on_a.wasm"
        )
        create_smartmodule(self.sm_name, sm_path)

    def tearDown(self):
        delete_topic(self.topic)
        delete_smartmodule(self.sm_name)

    def test_consume_with_smart_module_by_name(self):
        """
        Test adds a the alphabet into a topic in the format of record-[letter]

        A wasm smart module is added to the filter and a all messages are
        retrieved and stored in the records list We can then assert the
        following:

        - There should be 1 item
        - It should be record-a

        """

        wasm_module_path = os.path.abspath(
            "integration-tests/smartmodule_filter_on_a.wasm"
        )
        config = ConsumerConfig()
        config.wasmModulePath(wasm_module_path, SmartModuleKind.Filter)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in list(ascii_lowercase):
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


class TestFluvioMethods(unittest.TestCase):
    def setUp(self):
        self.topic = str(uuid.uuid4())
        create_topic(self.topic)

    def tearDown(self):
        delete_topic(self.topic)

    def test_connect(self):
        # A very simple test
        Fluvio.connect()

    def test_produce(self):
        fluvio = Fluvio.connect()

        producer = fluvio.topic_producer(self.topic)
        for i in range(10):
            producer.send_string("FOOBAR %s " % i)

    def test_consume_with_smart_module(self):
        """
        Test adds a the alphabet into a topic in the format of record-[letter]

        A wasm smart module is added to the filter and a all messages are
        retrieved and stored in the records list We can then assert the
        following:

        - There should be 1 item
        - It should be record-a

        """

        wasm_module_path = os.path.abspath(
            "integration-tests/smartmodule_filter_on_a.wasm"
        )
        config = ConsumerConfig()
        config.wasmModulePath(wasm_module_path, SmartModuleKind.Filter)

        fluvio = Fluvio.connect()
        producer = fluvio.topic_producer(self.topic)
        for i in list(ascii_lowercase):
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
            print("THIS IS IN AN ITERATOR! %s" % i.value())
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
            print("THIS IS IN AN ITERATOR! key - %s, value - %s" % (i.key(), i.value()))
            self.assertEqual(bytearray(i.value()).decode(), "record-%s" % count)
            self.assertEqual(i.value_string(), "record-%s" % count)
            self.assertEqual(i.key_string(), "foo")
            self.assertEqual(i.key(), list("foo".encode()))

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


class TestFluvioErrors(unittest.TestCase):
    def setUp(self):
        self.topic = str(uuid.uuid4())

    def test_produce_on_uncreated_topic(self):
        fluvio = Fluvio.connect()

        error = None
        try:
            fluvio.topic_producer(self.topic)
        except FluviorError as e:
            error = e
            print("ERROR: %s" % e)

        self.assertTrue(error is not None)
        self.assertEqual(
            error.args, ("Topic not found: %s" % self.topic,)  # noqa: E501
        )


class TestFluvioProduceFlush(unittest.TestCase):
    def setUp(self):
        self.topic = str(uuid.uuid4())
        create_topic(self.topic)

    def tearDown(self):
        delete_topic(self.topic)

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
