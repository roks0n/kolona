test:
	pytest .

format: isort black flake8 mypi

isort:
	isort kolona/ tests/

black:
	black .

flake8:
	flake8 .

mypi:
	mypy .

pip-compile:
	pip-compile requirements.in

pip-upgrade:
	pip-compile --upgrade requirements.in
