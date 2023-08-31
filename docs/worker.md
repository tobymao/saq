# Workers

TODO: Discuss running workers

## Command-line tool
```text
usage: saq [-h] [--workers WORKERS] [--verbose] [--web]
           [--extra-web-settings EXTRA_WEB_SETTINGS] [--port PORT] [--check]
           [--quiet]
           settings

Start Simple Async Queue Worker

positional arguments:
  settings              Namespaced variable containing worker settings eg: eg
                        module_a.settings

options:
  -h, --help            show this help message and exit
  --workers WORKERS     Number of worker processes
  --verbose, -v         Logging level: 0: ERROR, 1: INFO, 2: DEBUG
  --web                 Start web app. By default, this only monitors the
                        current worker's queue. To monitor multiple queues,
                        see '--extra-web-settings'
  --extra-web-settings EXTRA_WEB_SETTINGS, -e EXTRA_WEB_SETTINGS
                        Additional worker settings to monitor in the web app
  --port PORT           Web app port, defaults to 8080
  --check               Perform a health check
  --quiet, -q           Disable automatic logging configuration

environment variables:
  AUTH_USER            Basic auth user, defaults to admin
  AUTH_PASSWORD        Basic auth password, if not specified, no auth will be
                       used
```
