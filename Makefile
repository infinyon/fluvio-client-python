.PHONY: venv venv-pip

PYTHON=./venv/bin/python
PIP=./venv/bin/pip

venv:
	python -m venv venv

venv-pip: venv
	$(PIP) install -U pip setuptools
	$(PIP) install -r requirements.txt

build-wheel: venv-pip
	$(PYTHON) setup.py bdist_wheel

build-dev: venv-pip
	$(PYTHON) setup.py develop

test: build-dev
	fluvio topic create my-topic-iterator || true
	fluvio topic create my-topic-while || true
	fluvio topic create my-topic-produce || true
	$(PYTHON) setup.py test

ci-build: venv-pip
	CIBW_SKIP="cp27-*" $(PYTHON) -m cibuildwheel --platform linux --output-dir wheelhouse


clean:
	rm -r venv fluvio/fluvio_rust.*.so target
