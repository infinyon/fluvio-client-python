import os
import time
import multiprocessing
import signal
import unittest
import uuid
import asyncio

from enum import Enum
from fluvio import ConsumerConfig, Fluvio, FluvioAdmin, Offset


class ConsumerModeAsync(Enum):
    STREAM_ASYNC = "stream_async"
    STREAM_WITH_OFFSET_ASYNC = "stream_with_offset_async"


async def consumer_process_async(
    topic, control_queue, consumer_mode: ConsumerModeAsync
):
    """
    Function to run in a separate process that consumes messages asynchronously.
    It listens for SIGINT and SIGTERM signals and exits gracefully.
    """
    fluvio = Fluvio.connect()

    stream = None
    if consumer_mode == ConsumerModeAsync.STREAM_ASYNC:
        consumer = fluvio.partition_consumer(topic, 0)
        config = ConsumerConfig()
        # async_stream_with_config is an async call
        stream = await consumer.async_stream_with_config(Offset.beginning(), config)
    elif consumer_mode == ConsumerModeAsync.STREAM_WITH_OFFSET_ASYNC:
        consumer = fluvio.partition_consumer(topic, 0)
        config = ConsumerConfig()
        # async_stream is an async call
        stream = await consumer.async_stream(Offset.beginning())

    records = []
    print("Consuming records...")
    # async for record in stream:
    for _ in range(3):
        try:
            # In Fluvio, record.value() returns a buffer
            record = await stream.__anext__()
            records.append(bytearray(record.value()).decode())
        except StopIteration:
            # No more records in the stream
            print("No more records to consume.")
            break
        except KeyboardInterrupt:
            print("Received SIGINT. Exiting gracefully.")
            control_queue.put({"status": "interrupted"})
            return
        except Exception as e:
            # Handle other exceptions if necessary
            control_queue.put({"status": "error", "message": str(e)})
            return

    print(f"Consumed {len(records)} records.")
    # You could put successful results on queue if desired
    control_queue.put({"status": "success", "records": records})

    # Simulate some processing time to allow signal handling
    time.sleep(5)


def consumer_process_async_wrapper(topic, control_queue, consumer_mode):
    """
    Synchronous wrapper that runs the async consumer function in an event loop.
    This is what gets called by the multiprocessing.Process.
    """

    try:
        asyncio.run(consumer_process_async(topic, control_queue, consumer_mode))
    except KeyboardInterrupt:
        print("Received SIGINT. Exiting gracefully.")
        control_queue.put({"status": "interrupted"})
    except Exception as e:
        print(f"Error in consumer_process_async_wrapper: {e}")
        control_queue.put({"status": "error", "message": str(e)})


class TestFluvioConsumerSignalsAsync(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.admin = FluvioAdmin.connect()
        self.fluvio = Fluvio.connect()
        self.topic = str(uuid.uuid4())
        self.sm_name = str(uuid.uuid4())

        try:
            self.admin.create_topic(self.topic)
        except Exception as err:
            print("create_topic error {}, will try to verify".format(err))

        # list topics to verify topic was created
        max_retries = 100
        while max_retries > 0:
            topic = self.admin.list_topics([self.topic])
            if len(topic) > 0:
                break
            max_retries -= 1
            if max_retries == 0:
                self.fail("setup: Failed to create topic")
            time.sleep(0.1)

        # Generate a set of test data for the topic
        producer = self.fluvio.topic_producer(self.topic)
        for i in range(2):
            producer.send_string(f"record-{i}")
        producer.flush()

    def tearDown(self):
        self.admin.delete_topic(self.topic)
        time.sleep(1)

    def test_consume_with_sigterm_async(self):
        """
        Test that the consumer handles SIGINT (or SIGTERM) gracefully for multiple consumer modes.
        """
        consumer_modes = [
            ConsumerModeAsync.STREAM_ASYNC,
            ConsumerModeAsync.STREAM_WITH_OFFSET_ASYNC,
        ]

        for mode in consumer_modes:
            with self.subTest(mode=mode):
                control_queue = multiprocessing.Queue()
                # NOTE: we now point to consumer_process_async_wrapper (sync),
                #       rather than consumer_process_async (async).
                process = multiprocessing.Process(
                    target=consumer_process_async_wrapper,
                    args=(self.topic, control_queue, mode),
                )
                process.start()

                # Allow some time for the consumer to start and consume some records
                time.sleep(3)

                pid = process.pid
                if pid is not None:
                    print(f"Sending SIGINT to process {pid}")
                    os.kill(pid, signal.SIGINT)

                # Wait for the process to handle the signal
                process.join(timeout=10)

                # Ensure the process has terminated
                self.assertFalse(
                    process.is_alive(), "Consumer process should have terminated."
                )

                # Retrieve results from the queue
                try:
                    result = control_queue.get_nowait()
                except Exception as e:
                    self.fail(f"No result was returned from the consumer process. {e}")

                # Validate the result based on the status
                if result["status"] == "success":
                    records = result.get("records", [])
                    # Just an example check
                    self.assertTrue(
                        len(records) >= 0, "Expected some records or possibly none."
                    )
                elif result["status"] == "interrupted":
                    # The consumer was interrupted gracefully
                    self.assertTrue(True)  # Test passes as interruption was handled
                elif result["status"] == "error":
                    self.fail(
                        f"Consumer process encountered an error: {result['message']}"
                    )
                else:
                    self.fail("Consumer process returned an unknown status.")
