from fluvio import (Fluvio, Offset, Record)
import json

fluvio = Fluvio.connect()
consumer = fluvio.partition_consumer('mqtt', 0)

wasm_module = "/Users/bencleary/Development/device-filter/target/wasm32-unknown-unknown/debug/device_filter.wasm"
stream = consumer.stream_with_config(Offset.beginning(), wasm_module)


class MQTTRecord:
    topic: str
    payload: bytearray

    def __init__(self, _from: Record) -> None:
        mqtt_record = json.loads(_from.value_string())
        self.topic = mqtt_record["mqtt_topic"]
        self.payload = bytearray(mqtt_record["payload"])
        self.clean_extra_bytes()

    def clean_extra_bytes(self):
        self.payload = self.payload.decode('ascii').strip().strip('\x00')


for i in stream:
    mqtt = MQTTRecord(i)
    print(type(mqtt.payload))
    print(json.loads(mqtt.payload))