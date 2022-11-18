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
	source .venv/bin/activate
	reqs

reqs:
	pip install -r requirements.txt

setup: python precommit venv

# Deploy the package

deploy:
	rm -rf ./dist
	python3 setup.py sdist
	twine upload --repository testpypi dist/*
