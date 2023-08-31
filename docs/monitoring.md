# Monitoring

SAQ comes with a simple UI for monitor workers and jobs:

```{figure-md}
![SAQ Web UI](web.png){width=100%}

SAQ Web UI
```

## Part of worker process

You can run it as part of the worker process:
```nasm
saq examples.simple.settings --web
```
which wil serve it on port 8080 by default. You can specify a custom port by adding `--port <portnum>`. e.g.:
```nasm
saq examples.simple.settings --web --port 7000
```

## Mounted in your own web service
You can also mount the Web UI as part of your own web service

### Starlette/FastAPI
Module {py:class}`saq.web.starlette` contains a starlette instance for use in anything that is derived from Starlette.

::::{tab-set}
:::{tab-item} FastAPI
```python
from fastapi import FastAPI
from saq.web.starlette import saq_web

app = FastAPI()

app.mount("/monitor", saq_web("/monitor", queues=all_the_queues_list))
```
:::

:::{tab-item} Starlette
```python
from saq.web.starlette import saq_web
from starlette.routing import Mount

routes = [
    ...
    Mount("/monitor", saq_web("/monitor", queues=all_the_queues_list))
]
```
:::
::::

```{eval-rst}
.. autoapifunction:: saq.web.starlette.saq_web
    :noindex:
```
