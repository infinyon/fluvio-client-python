.PHONY: venv venv-pip

PYTHON=$(PWD)/venv/bin/python
PIP=$(PWD)/venv/bin/pip

venv:
	python3 -m venv venv

venv-pip: venv
	$(PIP) install -U pip build setuptools pdoc flake8 ipdb
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
	$(PYTHON) -m build --wheel

install-wheel: build-wheel
	$(PIP) install --upgrade --force-reinstall --no-index --pre --find-links=dist/ fluvio

build-dev: venv-pip
	$(PYTHON) -m pip install --no-build-isolation -e .[dev]

unit-tests: build-dev
	cd tests/ && PYTHONPATH=.. $(PYTHON) -m unittest

integration-tests: build-dev
	cd integration-tests && PYTHONPATH=.. $(PYTHON) -m unittest -v

test-signals: build-dev
	cd integration-tests/ && PYTHONPATH=.. $(PYTHON) -m unittest test_signals.py

test-signals-async: build-dev
	cd integration-tests/ && PYTHONPATH=.. $(PYTHON) -m unittest test_signals_async.py

test-produce: build-dev
	cd integration-tests/ && PYTHONPATH=.. $(PYTHON) -m unittest test_produce.py

test-consume: build-dev
	cd integration-tests/ && PYTHONPATH=.. $(PYTHON) -m unittest test_consume.py

macos-ci-tests: build-dev
	cd macos-ci-tests && PYTHONPATH=.. $(PYTHON) -m unittest

manual-tests: build-dev
	cd manual-tests/ && PYTHONPATH=.. $(PYTHON) -m unittest

ci-build: # This is for testing builds
	CIBW_BUILD="cp311-manylinux_x86_64 cp311-manylinux_aarch64 cp311-macosx_x86_64 cp311-macosx_universal2 cp311-macosx_arm64"  CIBW_SKIP="cp27-*" CIBW_BEFORE_ALL_LINUX="{package}/tools/cibw_before_all_linux.sh"  $(PYTHON) -m cibuildwheel --platform linux --output-dir wheelhouse

PDOC_OPTS=--logo='https://www.fluvio.io/img/infinyon-gradient.png' --favicon='https://www.fluvio.io/images/favicon.ico'

docs-serve: venv-pip build-dev
	$(PYTHON) -m pdoc fluvio $(PDOC_OPTS)

docs-build: venv-pip build-dev
	$(PYTHON) -m pdoc fluvio -o docs/ $(PDOC_OPTS)

clean:
	rm -rf venv fluvio/*.so target dist build

changelog:
	# backfill from older
	# git cliff v0.15.2..HEAD -t v0.15.7 -p CHANGELOG.md

	# normal changelog update
	# git cliff -l -p ./CHANGELOG.md

	# info
	git cliff  -l
