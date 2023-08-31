---
sd_hide_title: true
---
# Overview

```{toctree}
:hidden:

getting_started
usage
comparison
changelog
contribute
```

## SAQ (Simple Async Queue) documentation

SAQ (Simple Async Queue) is a simple and performant job queueing framework built on top of asyncio and redis. It can be used for processing background jobs with workers. For example, you could use SAQ to schedule emails, execute long queries, or do expensive data analysis.

It uses [redis-py](https://github.com/redis/redis-py) >= 4.2.

It is similar to [RQ](https://github.com/rq/rq) and heavily inspired by [ARQ](https://github.com/samuelcolvin/arq). Unlike RQ, it is async and thus [significantly faster](#comparison) if your jobs are async. Even if they are not, SAQ is still considerably faster due to lower overhead.


::::{grid} 2
:gutter: 3
:margin: 1

:::{grid-item-card}  Getting Started
:link: getting_started
:link-type: doc

Getting started with SAQ?
:::

:::{grid-item-card}  Usage
:link: usage
:link-type: doc

Guides on how to use SAQ
:::

:::{grid-item-card}  API Reference
:link: autoapi/index
:link-type: doc

The API Reference for advanced users.
:::

:::{grid-item-card}  Changelog
:link: changelog
:link-type: doc

See the recent changes, and upgrade instuctions if needed.
:::
::::


## Indices and tables

* <project:#genindex>
* <project:#modindex>
