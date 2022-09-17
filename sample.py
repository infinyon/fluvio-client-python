from fluvio import (Fluvio, Offset)

fluvio = Fluvio.connect()
consumer = fluvio.partition_consumer('mqtt', 0)

# print(dir(consumer.stream_with_config))

stream = consumer.stream_with_config(Offset.beginning(), "/Users/bencleary/Development/device-filter/target/wasm32-unknown-unknown/debug/device_filter.wasm")
# stream = consumer.stream_with_config(Offset.beginning())

for i in stream:
    print(i.value_string())