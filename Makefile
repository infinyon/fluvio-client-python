.PHONY: venv venv-pip

PYTHON=./venv/bin/python
PIP=./venv/bin/pip

venv:
	python3 -m venv venv

venv-pip: venv
	$(PIP) install -U pip setuptools pdoc flake8 ipdb
	$(PIP) install -r requirements.txt

lint: venv-pip
	cargo fmt -- --check
	$(PYTHON) -m flake8 fluvio tests

build-wheel: venv-pip
	rm -rf ./fluvio.egg-info/
	$(PYTHON) setup.py bdist_wheel

install-wheel: build-wheel
	$(PIP) install --upgrade --force-reinstall --no-index --pre --find-links=dist/ fluvio

build-dev: venv-pip
	$(PYTHON) setup.py develop

test: install-wheel
	cd tests && ../venv/bin/python -m unittest

ci-build: venv-pip
	CIBW_SKIP="cp27-*" $(PYTHON) -m cibuildwheel --platform linux --output-dir wheelhouse

docs-serve: venv-pip build-dev
	$(PYTHON) -m pdoc fluvio

docs-build: venv-pip build-dev
	$(PYTHON) -m pdoc fluvio -o docs

clean:
	rm -rf venv fluvio/*.so target dist build
