# Tasks

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


```{py:function} task_type(ctx: saq.types.Context, *, **kwargs) -> JSONType:

Basic task type

:param saq.types.Context ctx: The task context
:param kwargs: Task parameters
:rtype: Any JSON serialisable
```

Discuss that if you configured retries that it will automatically retry on any Exception except asyncio.CancelledError
