# Welcome to SAQ's documentation!

```{toctree}
:hidden:

getting_started
usage
comparison
benchmarks
```

SAQ (Simple Async Queue) is a simple and performant job queueing framework built on top of asyncio and redis. It can be used for processing background jobs with workers. For example, you could use SAQ to schedule emails, execute long queries, or do expensive data analysis.

It uses [redis-py](https://github.com/redis/redis-py) >= 4.2.

It is similar to [RQ](https://github.com/rq/rq) and heavily inspired by [ARQ](https://github.com/samuelcolvin/arq). Unlike RQ, it is async and thus [significantly faster](#benchmarks) if your jobs are async. Even if they are not, SAQ is still considerably faster due to lower overhead.

SAQ optionally comes with a simple UI for monitor workers and jobs:

```{figure-md}
![SAQ Web UI](web.png){width=100%}

SAQ Web UI
```

# Indices and tables

* <project:#genindex>
* <project:#modindex>
