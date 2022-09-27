<h1 align="center">Fluvio Client for Python</h1>
<div align="center">
 <strong>
   Python binding for Fluvio streaming platform.
 </strong>
</div>
<br />

[![Build](https://github.com/infinyon/fluvio-client-python/actions/workflows/cloud.yml/badge.svg)](https://github.com/infinyon/fluvio-client-python/actions/workflows/cloud.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/infinyon/fluvio-client-python/blob/master/LICENSE-APACHE)
[![PyPi](https://img.shields.io/pypi/v/fluvio.svg)](https://img.shields.io/pypi/v/fluvio.svg)

## Documentation

Fluvio client uses [pdoc](https://github.com/mitmproxy/pdoc) to generate the client API [documentation](https://infinyon.github.io/fluvio-client-python/fluvio.html).

## Installation

```
pip install fluvio
```

This will get the wheel for the os/architecture of the installation system if available, otherwise it will try to build from source. If building from source, you will need the rust compiler and maybe some operating system sources.

# Example Usage

## Producer
```python
from fluvio import Fluvio
fluvio = Fluvio.connect()
producer = fluvio.topic_producer('my-topic')
producer.send_string("FOOBAR")
```

## Consumer
```python
from fluvio import (Fluvio, Offset)
fluvio = Fluvio.connect()
consumer = fluvio.partition_consumer('my-topic-while', 0)
stream = consumer.stream(Offset.beginning())

for i in stream:
    print(i.value_string())
```

# Developer Notes

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

You should probably stick to using `make integration-tests` which will create the [virtual
environment](https://docs.python.org/3/tutorial/venv.html) and install the
package in the site-packages in the venv directory. This makes sure that the
package is also packaged correctly.

If you'd like more rapid testing, once you've got the virtual environment
activated, `python setup.py test` will compile the rust as a static library and
put it as `fluvio/fluvio_python.cpython-39-x86_64-linux-gnu.so`. This filename
is dependent on the host OS and python version.
FLUVIO_CLOUD_TEST_PASSWORD` to your fork's secrets.

When submitting a PR, CI checks a few things:
* `make integration-tests` against a fluvio cluster in CI.
* `make macos-ci-tests` with no fluvio cluster present (the macOS github runner is flakey) to verify linking is done correctly.
* `make lint`. This checks that [`cargo
fmt`](https://github.com/rust-lang/rustfmt),
[`flake8`](https://pypi.org/project/flake8) and
[`black`](https://pypi.org/project/black/) are all clear.
