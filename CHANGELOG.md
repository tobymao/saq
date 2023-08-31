# Changelog

## v0.11

### v0.11.2
- Added package `saq.web` to build

### v0.11.1
- Added embeddable `saq.web.starlette`

### v0.11.0
- `saq` command-line client includes current path to simplify development
- Fetch outstanding jobs from incomplete namespace instead of queued.
- Documentation now hosted on [Read The Docs](https://saq-py.readthedocs.io/)

## v0.10
- Always use FIFO queue behavior

## v0.9

### v0.9.4
- Update redis requirements

### v0.9.3
- Fix timeout of 0
- Allow `apply()` to take a negative ttl
- Clean up signal handlers after shutdown event is triggered

### v0.9.2
- Job context is more dynamic
- Limit redis to prevent breakage

### v0.9.1
- Fix worker queue settings
- Better type info for `job_keys`
- Add type annotations
- Fixes ResourceWarning in tests

### v0.9.0
- basic auth fixes

## v0.8

### v0.8.1
- Replace exception logger function with `logger.exception`
- Correct param name in docstring for `Worker`
- Documentation fixes

### v0.8.0
- Add `--quiet` to command-line client
- Allow users to expire, keep, or disable storage of job information

## v0.7

### v0.7.1
- Fix bug with queue map timeouts

### v0.7.0
- More robust error handling

## v0.6

### v0.6.2
- Update readme

### v0.6.1
- Make upkeep more robust to errors

### v0.6.0
- Use queue name in the job key
