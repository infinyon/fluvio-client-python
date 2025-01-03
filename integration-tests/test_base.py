import unittest
import uuid
import time

from fluvio import FluvioAdmin, Fluvio


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


class CommonFluvioSetup(unittest.TestCase):
    def common_setup(self, sm_path=None):
        """Optionally create a smartmodule if sm_path is provided"""
        self.admin = FluvioAdmin.connect()
        self.fluvio = Fluvio.connect()
        self.topic = str(uuid.uuid4())
        self.consumer_id = str(uuid.uuid4())
        self.sm_name = str(uuid.uuid4())
        self.sm_path = sm_path

        if sm_path is not None:
            create_smartmodule(self.sm_name, sm_path)

        try:
            self.admin.create_topic(self.topic)
        except Exception as err:
            print("create_topic error {}, will try to verify", err)

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

    def setUp(self):
        self.common_setup()

    def tearDown(self):
        self.admin.delete_topic(self.topic)
        # TODO: we can remove this delete_consumer_offset after to fix
        # this bug: https://github.com/infinyon/fluvio/issues/4308
        self.fluvio.delete_consumer_offset(self.consumer_id, self.topic, 0)
        time.sleep(1)
        if self.sm_path is not None:
            self.admin.delete_smartmodule(self.sm_name)
