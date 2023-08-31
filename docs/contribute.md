# Contribution

## Development
```sh
python -m venv env
source env/bin/activate
pip install -e ".[dev,web]"
docker run -p 6379:6379 redis
./run_checks.sh
```

## Makefile
SAQ has a `Makefile` which is used to simplify developing on the library:

```text
tobymao/saq dev makefile

usage: make <target>
 up             Force updates dev/test dependencies - attempts clean-install
 deps           Ensure dev/test dependencies are installed
 test           Runs all tests
 lint           Reports all linter violations
 ci             Runs lints & tests (as a full CI run would)
 format         Tries to auto-fix simpler linting issues
 devdocs        Builds docs and hosts them on port 8000
```
