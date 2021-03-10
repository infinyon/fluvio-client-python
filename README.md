<h1 align="center">Fluvio Client for Python</h1>
<div align="center">
 <strong>
   Python binding for Fluvio streaming platform.
 </strong>
</div>
<br />

[![Build Status](https://github.com/infinyon/fluvio-client-python/workflows/CI/badge.svg)](https://github.com/infinyon/flv-client-python/actions) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/infinyon/flv-client-python/blob/master/LICENSE-APACHE) [![PyPi](https://img.shields.io/pypi/v/fluvio.svg)](https://img.shields.io/pypi/v/fluvio.svg)

# Usage

## Producer
```python
from fluvio import Fluvio
fluvio = Fluvio.connect()
producer = fluvio.topic_producer('my-topic')
producer.send_record("FOOBAR", 0)
```

## Consumer
```python
from fluvio import Fluvio
fluvio = Fluvio.connect()
consumer = fluvio.partition_consumer('my-topic-while', 0)
stream = consumer.stream(0)
```
Given the `stream` instance, you can use it by calling `.next()` like this:
```python
curr = stream.next()
while curr is not None:
    print(curr)
```

Or we've added an iterator wrapped around it:
```python
for i in PartitionConsumerStreamIterator(stream):
    print(i)
```

# Development Notes

This project uses [flapigen](https://github.com/Dushistov/flapigen-rs) to
genate the C static library and
[setuptools-rust](https://github.com/PyO3/setuptools-rust) to bundle it into a
python package. For cross platform builds,
       [cibuildwheel](https://github.com/joerick/cibuildwheel) is used.

Running the tests locally require having already setup a [fluvio
locally](https://www.fluvio.io/docs/getting-started/fluvio-local/) or on
[fluvio cloud](https://cloud.fluvio.io).


Add python unit tests in the `tests` directory using the built in python
[`unittest` framework](https://docs.python.org/3/library/unittest.html)

You should probably stick to using `make test` which will create the [virtual
environment](https://docs.python.org/3/tutorial/venv.html) and install the
package in the site-packages in the venv directory. This makes sure that the
package is also packaged correctly.

If you'd like more rapid testing, once you've got the virtual environment
activated, `python setup.py test` will compile the rust as a static library and
put it as `fluvio/fluvio_python.cpython-39-x86_64-linux-gnu.so`. This filename
is dependent on the host OS and python version.
