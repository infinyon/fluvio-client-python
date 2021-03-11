.PHONY: venv venv-pip

PYTHON=./venv/bin/python
PIP=./venv/bin/pip

venv:
	python3 -m venv venv

venv-pip: venv
	$(PIP) install -U pip setuptools pdoc flake8
	$(PIP) install -r requirements.txt

lint: venv-pip
	$(PYTHON) -m flake8 fluvio tests

build-wheel: venv-pip
	rm -rf ./fluvio.egg-info/
	$(PYTHON) setup.py bdist_wheel

install-wheel: build-wheel
	#rm -r ./venv/lib/python3.9/site-packages/fluvio*
	$(PIP) install --upgrade --force-reinstall --no-index --pre --find-links=dist/ fluvio

build-dev: venv-pip
	$(PYTHON) setup.py develop

test: install-wheel
	fluvio topic create my-topic-iterator || true
	fluvio topic create my-topic-while || true
	fluvio topic create my-topic-produce || true
	cd tests && ../venv/bin/python -m unittest
	fluvio topic delete my-topic-iterator || true
	fluvio topic delete my-topic-while || true
	fluvio topic delete my-topic-produce || true

ci-build: venv-pip
	CIBW_SKIP="cp27-*" $(PYTHON) -m cibuildwheel --platform linux --output-dir wheelhouse

docs-serve: venv-pip build-dev
	$(PYTHON) -m pdoc fluvio

clean:
	rm -rf venv fluvio/*.so target
