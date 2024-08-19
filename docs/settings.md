# Settings

TODO: Discuss settings dict and options available

```python
settings_raw_dict = {
    "queue": queue,
    "functions": [test],
    "concurrency": 10,
    "cron_jobs": [CronJob(cron, cron="* * * * * */5")], # run every 5 seconds
    "startup": startup,
    "shutdown": shutdown,
    "before_process": before_process,
    "after_process": after_process,
}
settings_typed_dict = SettingsDict(
    queue=queue,
    functions=[test],
    concurrency=10,
    cron_jobs=[CronJob(cron, cron="* * * * * */5")], # run every 5 seconds
    startup=startup,
    shutdown=shutdown,
    before_process=before_process,
    after_process=after_process,
)
```
