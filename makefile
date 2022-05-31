test:
	pytest .

format: isort black flake8 mypi

isort:
	isort kolona/ tests/

black:
	black .

flake8:
	flake8 .

mypy:
	mypy setup.py kolona

pip-compile:
	pip-compile requirements.in

pip-upgrade:
	pip-compile --upgrade requirements.in

cleanup-dist:
	rm -rf dist

build-package:
	python -m build

upload-package:
	python -m twine upload dist/*
