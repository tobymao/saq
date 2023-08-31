# Jobs

Jobs can be scheduled to run as:
* Fire-and-forget tasks: {py:class}`saq.queue.Queue.enqueue`
* Wait-for-result tasks: {py:class}`saq.queue.Queue.apply`

Sample code:
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

## Common

### Explicit vs Implicit job calling
TODO: queue.enqueue(Job(...)) vs known-parameters

### Job defaults
TODO: Discuss example to subclass Queue to configure defaults for enqueue (e.g. retries)

### Retries
TODOL Discuss that retries ALWAYS jitter

## Enqueue
```{eval-rst}
.. autoapifunction:: saq.queue.Queue.enqueue
    :noindex:
```

## Apply
```{eval-rst}
.. autoapifunction:: saq.queue.Queue.apply
    :noindex:
```

## Map
```{eval-rst}
.. autoapifunction:: saq.queue.Queue.map
    :noindex:
```

## Batch
```{eval-rst}
.. autoapifunction:: saq.queue.Queue.batch
    :noindex:
```
