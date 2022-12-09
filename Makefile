PYTHON_VERSION=3.10.6

# Setup commands

python:
	pyenv install --skip-existing $(PYTHON_VERSION)
	echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
	pyenv local $(PYTHON_VERSION)

precommit:
	pre-commit install

venv:
	python3 -m venv .venv
	. .venv/bin/activate
	make reqs

reqs:
	pip install -r requirements.txt

setup: python precommit venv

# Deploy the package

deploy/test:
	rm -rf ./dist
	python3 setup.py sdist
	twine upload --repository testpypi dist/*

deploy/release:
	rm -rf ./dist
	python3 setup.py sdist
	twine upload dist/*

documentation/build:
	sphinx-apidoc -o ./docs/source ./rapid
	cd ./docs && $(MAKE) html

documentation/serve:
	python -m http.server --directory ./docs/build/html
