PATHS = saq/ tests/ setup.py

help:
	@echo  "tobymao/saq dev makefile"
	@echo  ""
	@echo  "usage: make <target>"
	@echo  " up	Updates dev/test dependencies"
	@echo  " deps	Ensure dev/test dependencies are installed"
	@echo  " test	Runs all tests"
	@echo  " lint	Reports all linter violations"
	@echo  " ci	Runs lints & tests (as a full CI run would)"
	@echo  " format Tries to auto-fix simpler linting issues"

up:
	pip-compile -qU tests/requirements.in
	sed -i 's/^-e .*/-e ./' tests/requirements.txt
	pip-sync tests/requirements.txt

deps:
	@which pip-sync > /dev/null || pip install pip-tools
	pip-sync -q tests/requirements.txt

test:
	python -m green

lint:
	python -m pylint ${PATHS}
	python -m black ${PATHS}
	python -m mypy ${PATHS}

format:
	python -m black ${PATHS}

ci: deps lint test

run:
	./etc/devrun.sh
