# Getting Started

## Install

### Minimal install
```nasm
pip install saq
```

### Web + hiredis
```nasm
pip install saq[web,hiredis]
```


## Usage
```text
usage: saq [-h] [--workers WORKERS] [--verbose] [--web]
           [--extra-web-settings EXTRA_WEB_SETTINGS]
           [--port PORT] [--check]
           settings

Start Simple Async Queue Worker

positional arguments:
  settings              Namespaced variable containing
                        worker settings eg: eg
                        module_a.settings

options:
  -h, --help            show this help message and exit
  --workers WORKERS     Number of worker processes
  --verbose, -v         Logging level: 0: ERROR, 1: INFO,
                        2: DEBUG
  --web                 Start web app. By default, this
                        only monitors the current
                        worker's queue. To monitor
                        multiple queues, see '--extra-
                        web-settings'
  --extra-web-settings EXTRA_WEB_SETTINGS, -e EXTRA_WEB_SETTINGS
                        Additional worker settings to
                        monitor in the web app
  --port PORT           Web app port, defaults to 8080
  --check               Perform a health check

environment variables:
  AUTH_USER     basic auth user, defaults to admin
  AUTH_PASSWORD basic auth password, if not specified, no auth will be used
```

## Example
```python
import asyncio

from saq import CronJob, Queue

# all functions take in context dict and kwargs
async def test(ctx, *, a):
    await asyncio.sleep(0.5)
    # result should be json serializable
    # custom serializers and deserializers can be used through Queue(dump=,load=)
    return {"x": a}

async def cron(ctx):
  print("i am a cron job")

async def startup(ctx):
    ctx["db"] = await create_db()

async def shutdown(ctx):
    await ctx["db"].disconnect()

async def before_process(ctx):
    print(ctx["job"], ctx["db"])

async def after_process(ctx):
    pass

queue = Queue.from_url("redis://localhost")

settings = {
    "queue": queue,
    "functions": [test],
    "concurrency": 10,
    "cron_jobs": [CronJob(cron, cron="* * * * * */5")], # run every 5 seconds
    "startup": startup,
    "shutdown": shutdown,
    "before_process": before_process,
    "after_process": after_process,
}
```

To start the worker, assuming the previous is available in the python path

```nasm
saq module.file.settings
```

To enqueue jobs

```python
# schedule a job normally
job = await queue.enqueue("test", a=1)

# wait 1 second for the job to complete
await job.refresh(1)
print(job.results)

# run a job and return the result
print(await queue.apply("test", a=2))

# schedule a job in 10 seconds
await queue.enqueue("test", a=1, scheduled=time.time() + 10)
```

## Demo

Start the worker

```nasm
saq examples.simple.settings --web
```

Navigate to the [web ui](http://localhost:8080])

Enqueue jobs
```nasm
python examples/simple.py
```

## Development
```
python -m venv env
source env/bin/activate
pip install -e ".[dev,web]"
docker run -p 6379:6379 redis
./run_checks.sh
```
