PATHS = saq/ tests/ setup.py
INSTALL = -e .[hiredis,web,dev]

help:
	@echo  "tobymao/saq dev makefile"
	@echo  ""
	@echo  "usage: make <target>"
	@echo  " up	Force updates dev/test dependencies - attempts clean-install"
	@echo  " deps	Ensure dev/test dependencies are installed"
	@echo  " test	Runs all tests"
	@echo  " lint	Reports all linter violations"
	@echo  " ci	Runs lints & tests (as a full CI run would)"
	@echo  " format Tries to auto-fix simpler linting issues"

up:
	pip freeze | grep -v "^-e" | xargs pip uninstall -y
	pip install -U --upgrade-strategy eager ${INSTALL}

deps:
	pip install -q ${INSTALL}

deps_docs:
	pip install -q -r docs/requirements.txt

test:
	@python -m coverage erase
	python -m coverage run -m unittest
	@python -m coverage report

lint:
	python -m pylint ${PATHS}
	python -m black ${PATHS}
	python -m mypy ${PATHS}

format:
	python -m black ${PATHS}

ci: deps lint test

run:
	./etc/devrun.sh

devdocs: deps_docs
	sphinx-autobuild docs docs/_build/html
