# Jobs

To enqueue a job:

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

Discuss simple enque and specifying extra options and using Job()

queue.enqueue(Job(...))

Discuss example to subclass Queue to configure defaults for enqueue (e.g. retries)

Discuss that retries ALWAYS jitter
