# Tasks

In SAQ, the concepts of **Task** and **Job** are separate but related. Understanding this distinction is key to using the library effectively.

*   A **Task** is the blueprint for your work. It is a Python function that you define in your codebase. It contains the logic to be executed.
*   A **Job** is a specific execution of a Task. It is created when you enqueue a task with specific arguments. You can create millions of Jobs from a single Task definition.

## Defining a Task

A task is a Python function that accepts a `Context` object as its first argument, followed by any keyword arguments your task requires. The function can be synchronous or asynchronous, but `async def` is recommended to take full advantage of SAQ's non-blocking capabilities.

The return value of a task must be JSON serializable.

## Task spec

```{py:function} task_type(ctx: saq.types.Context, *, **kwargs) -> JSONType:

Basic task type

:param saq.types.Context ctx: The task context
:param kwargs: Task parameters
:rtype: Any JSON serialisable
```

Example:
```python
import asyncio

from saq.types import Context

# All functions take in context dict and kwargs
async def double(ctx: Context, *, val: int) -> int:
    await asyncio.sleep(0.5)

    # Result should be json serializable
    return val * 2
```

```python
import asyncio
from saq.types import Context

# A task is a function that takes a context and keyword arguments.
async def send_welcome_email(ctx: Context, *, user_id: int) -> dict:
    print(f"Attempting to send email to user {user_id}...")
    await asyncio.sleep(1)  # Simulate network I/O
    return {"status": "sent", "user_id": user_id}
```

## Registering Tasks with a Worker

A task function is just a piece of Python code until you register it with a Worker. The Worker needs to know which function to run when it receives a job from the queue.

You register tasks by passing a list of functions to the Worker constructor. SAQ uses the function's qualified name (e.g., "mymodule.send_welcome_email") as its unique identifier.

```python
from saq.worker import Worker
from saq.queue import Queue

# Import the task function
from .tasks import send_welcome_email

queue = Queue.from_url("redis://localhost")

# The worker needs to know about the task functions it can execute.
worker = Worker(queue=queue, functions=[send_welcome_email])
```

## Enqueueing a Job

To execute a task, you create a Job by calling `queue.enqueue()`. You pass the string name of the task and the keyword arguments it needs.

```python
# Enqueue a job to be processed by a worker.
# This is a "fire-and-forget" operation.
job = await queue.enqueue(
    "send_welcome_email",
    user_id=123,
)
if job:
    print(f"Enqueued job {job.key} for task 'send_welcome_email'")
```

## Controlling Job Execution

When you enqueue a job, you can pass several arguments to control its behavior. These arguments are defined on the Job class. Here are the key arguments:

*   `key`: A unique key to prevent duplicate jobs from being enqueued.
*   `timeout`: Maximum time in seconds the job is allowed to run. Defaults to 10.
*   `heartbeat`: Maximum amount of time a job can survive without a heartbeat in seconds. Defaults to 0.
*   `retries`: Maximum number of times to retry the job if it fails. Defaults to 1.
*   `retry_delay`: Seconds to wait before the next retry.
*   `retry_backoff`: If True, use exponential backoff for retry delays, doubling the delay after each attempt.
*   `scheduled`: A future timestamp (epoch seconds) to schedule the job for.

```python
# Enqueue a job with custom execution settings
await queue.enqueue(
    "send_welcome_email",
    user_id=456,
    retries=5,
    timeout=60,
    retry_delay=5.0,
    retry_backoff=True,
)
```

## The Context Object

Every task receives a Context object as its first argument. This is a dictionary-like object that provides access to runtime information about the job's execution.

The most important keys are:

*   `ctx['job']`: The Job instance being executed. This gives you access to all its properties like key, attempts, and meta.
*   `ctx['worker']`: The Worker instance processing the job.
*   `ctx['queue']`: The Queue the job was pulled from.
*   `ctx['exception']`: If the task is being retried, this will hold the exception from the previous failed attempt.
*   You can use the context to enrich your task's logic or perform actions like updating the job's progress.

## Fetching Results with apply

If you need to enqueue a job and wait for its result, use `queue.apply()`. This will block until the job is complete and return its result. If the job fails, it will raise a JobError.

```python
from saq.job import JobError

async def add(ctx, a, b):
    return a + b

# ... worker setup ...

try:
    result = await queue.apply("add", a=5, b=10)
    print(f"Result: {result}")  # Prints "Result: 15"
except JobError as e:
    print(f"Job failed: {e.job.error}")
```

## Recurring Tasks (Cron Jobs)

To run tasks on a recurring schedule, you can define a CronJob and pass it to the Worker. This uses standard cron syntax.

```python
from saq.job import CronJob

async def cleanup_task(ctx):
    print("Performing nightly cleanup...")

cron_jobs = [
    # Run every day at midnight
    CronJob(cleanup_task, cron="0 0 * * *")
]

worker = Worker(queue=queue, functions=[cleanup_task], cron_jobs=cron_jobs)
```
