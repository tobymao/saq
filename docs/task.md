# Tasks

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

## Retries
If you configured retries when enqueueing, your task will automatically retry on any {py:class}`Exception` except {py:class}`asyncio.CancelledError`

## Context
TODO: Discuss context, and enriching context
