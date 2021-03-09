.PHONY: venv venv-pip

PYTHON=./venv/bin/python
PIP=./venv/bin/pip

venv:
	python3 -m venv venv

venv-pip: venv
	$(PIP) install -U pip setuptools
	$(PIP) install -r requirements.txt

build-wheel: venv-pip
	$(PYTHON) setup.py bdist_wheel

install-wheel: build-wheel
	#rm -r ./venv/lib/python3.9/site-packages/fluvio*
	$(PIP) install --upgrade --force-reinstall --no-index --pre --find-links=dist/ fluvio

build-dev: venv-pip
	#$(PYTHON) setup.py develop
	$(PYTHON) setup.py install

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


clean:
	rm -r venv fluvio/*.so target
