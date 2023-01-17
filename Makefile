.PHONY: venv venv-pip

PYTHON=$(PWD)/venv/bin/python
PIP=$(PWD)/venv/bin/pip

venv:
	python3 -m venv venv

venv-pip: venv
	$(PIP) install -U pip setuptools pdoc flake8 ipdb
	$(PIP) install -r requirements.txt
	$(PYTHON) --version
	$(PIP) --version

lint: venv-pip
	cargo fmt -- --check
	$(PYTHON) -m flake8 fluvio integration-tests macos-ci-tests
	$(PYTHON) -m black --check fluvio integration-tests macos-ci-tests

lint-write: venv-pip
	cargo fmt
	$(PYTHON) -m black fluvio integration-tests macos-ci-tests

build-wheel: venv-pip
	rm -rf ./fluvio.egg-info/
	$(PYTHON) setup.py bdist_wheel

install-wheel: build-wheel
	$(PIP) install --upgrade --force-reinstall --no-index --pre --find-links=dist/ fluvio

build-dev: venv-pip
	$(PYTHON) setup.py develop

integration-tests: build-dev
	$(PYTHON) setup.py test

macos-ci-tests: build-dev
	cd macos-ci-tests && $(PYTHON) -m unittest

manual-tests: build-dev
	cd manual-tests/ && $(PYTHON) -m unittest

ci-build: # This is for testing builds
	CIBW_BUILD="cp311-manylinux_x86_64 cp311-manylinux_aarch64 cp311-macosx_x86_64 cp311-macosx_universal2 cp311-macosx_arm64"  CIBW_SKIP="cp27-*" CIBW_BEFORE_ALL_LINUX="{package}/tools/cibw_before_all_linux.sh"  $(PYTHON) -m cibuildwheel --platform linux --output-dir wheelhouse

manylinux2014_aarch64:
	docker build -f cross/Dockerfile.aarch64-unknown-linux-gnu -t fluvio-cross-python:aarch64-unknown-linux-gnu cross
	python setup.py bdist_wheel --py-limited-api=cp38 --plat-name manylinux2014_aarch64

manylinux2014_x86_64:
	python setup.py bdist_wheel --py-limited-api=cp38 --plat-name manylinux2014_x86_64

docs-serve: venv-pip build-dev
	$(PYTHON) -m pdoc fluvio

docs-build: venv-pip build-dev
	$(PYTHON) -m pdoc fluvio -o docs

clean:
	rm -rf venv fluvio/*.so target dist build
