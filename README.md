# SAQ
SAQ (Simple Async Queue) is a simple and performant job queueing framework built on top of asyncio and redis.

It is inspired by [ARQ](https://github.com/samuelcolvin/arq) but has several enhancements.

1. Avoids polling by leveraging [BLMOVE](https://redis.io/commands/blmove) or [RPOPLPUSH](https://redis.io/commands/rpoplpush) and NOTIFY
    1. SAQ has much lower latency than ARQ
2. Web interface for monitoring queues and workers
3. Heartbeat monitor for abandoned jobs
4. More robust failure handling
    1. Storage of stack traces
    2. Sweeping stuck jobs
    3. Handling of cancelled jobs different from failed jobs (machine redeployments)
5. Before and after job hooks

## Install
```
# minimal install
pip install saq

# web + hiredis
pip install saq[web,hiredis]
```

## Usage
```
usage: saq [-h] [--workers WORKERS] [--verbose] [--web] settings

Start Simple Async Queue Worker

positional arguments:
  settings           Namespaced variable containing worker settings eg: eg module_a.settings

options:
  -h, --help         show this help message and exit
  --workers WORKERS  Number of worker processes
  --verbose, -v      Logging level: 0: ERROR, 1: INFO, 2: DEBUG
  --web              Start web app
```

## Development
```
python -m venv env
source env/bin/activate
pip install -e .[dev,web]
docker run -p 6379:6379 redis
./run_checks.sh
```
