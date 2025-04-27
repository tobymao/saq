PATHS = saq/ tests/
INSTALL = -e .[hiredis,web,dev,redis,postgres]

help:
	@echo  "tobymao/saq dev makefile"
	@echo  ""
	@echo  "usage: make <target>"
	@echo  " up		Force updates dev/test dependencies - attempts clean-install"
	@echo  " deps		Ensure dev/test dependencies are installed"
	@echo  " test		Runs all tests"
	@echo  " style		Lint, types, and formatting"
	@echo  " ci		Runs style & tests (as a full CI run would)"
	@echo  " devdocs	Builds docs and hosts them on port 8000"

up:
	pip freeze | grep -v "^-e" | xargs pip uninstall -y
	pip install -U --upgrade-strategy eager ${INSTALL}

deps:
	pip install -q ${INSTALL}

deps_docs:
	pip install -q -r docs/requirements.txt

test:
	@python -m coverage erase
	python -m coverage run -m unittest -vv
	@python -m coverage report

style:
	pre-commit run --all-files

ci: deps style test

devdocs: deps_docs
	sphinx-autobuild --ignore '*/changelog.md' docs docs/_build/html
