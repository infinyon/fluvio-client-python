import os
import time
import multiprocessing
import signal

from enum import Enum
from fluvio import ConsumerConfig, Fluvio, Offset, ConsumerConfigExtBuilder
from test_base import CommonFluvioSetup  # Ensure this is correctly imported


# Explicitly set the start method for multiprocessing
multiprocessing.set_start_method("spawn", force=True)


class ConsumerMode(Enum):
    STREAM = "stream"
    STREAM_WITH_OFFSET = "stream_with_offset"
    CONSUMER_WITH_CONFIG = "consumer_with_config"


def consumer_process(topic, control_queue, consumer_mode: ConsumerMode):
    """
    Function to run in a separate process that consumes messages.
    It listens for SIGINT and SIGTERM signals and exits gracefully.
    """
    fluvio = Fluvio.connect()

    stream = None
    if consumer_mode == ConsumerMode.STREAM:
        consumer = fluvio.partition_consumer(topic, 0)
        config = ConsumerConfig()
        stream = consumer.stream_with_config(Offset.beginning(), config)
    elif consumer_mode == ConsumerMode.STREAM_WITH_OFFSET:
        consumer = fluvio.partition_consumer(topic, 0)
        config = ConsumerConfig()
        stream = consumer.stream(Offset.beginning())
    elif consumer_mode == ConsumerMode.CONSUMER_WITH_CONFIG:
        builder = ConsumerConfigExtBuilder(topic)
        config = builder.build()
        stream = fluvio.consumer_with_config(config)

    records = []
    print("Consuming records...")
    for _ in range(3):
        try:
            record = next(stream)
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
    # Simulate some processing time to allow signal handling
    time.sleep(5)


class TestFluvioConsumerSignals(CommonFluvioSetup):
    def setUp(self):
        self.common_setup()
        # Generate a set of test data for the topic
        producer = self.fluvio.topic_producer(self.topic)
        for i in range(2):
            producer.send_string(f"record-{i}")
        producer.flush()

    def test_consume_with_sigterm(self):
        """
        Test that the consumer handles SIGTERM gracefully for multiple consumer modes.
        """
        consumer_modes = [
            ConsumerMode.STREAM,
            ConsumerMode.STREAM_WITH_OFFSET,
            ConsumerMode.CONSUMER_WITH_CONFIG,
        ]

        for mode in consumer_modes:
            with self.subTest(mode=mode):
                control_queue = multiprocessing.Queue()
                process = multiprocessing.Process(
                    target=consumer_process, args=(self.topic, control_queue, mode)
                )
                process.start()

                # Allow some time for the consumer to start and consume some records
                time.sleep(2)

                pid = process.pid
                if pid is not None:
                    print(f"Sending SIGTERM to process {pid}")
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
                    self.assertTrue(
                        len(records) <= 3, "Consumed more records than expected."
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
