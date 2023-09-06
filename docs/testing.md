# Testing

SAQ provides a basic test double that can make writing your tests a bit more convenient.

## Usage
If your code only uses {py:class}`saq.queue.Queue.enqueue`:
```py
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from saq.testing import TestQueue
from tests.testing.tasks import enqueues_a_job

# If you want to assert queue behavior
with patch("tests.testing.tasks.queue", new=TestQueue()) as testqueue:
    await enqueues_a_job()
    testqueue.assertEnqueuedTimes("thetask", 1)

# You can also just wrap a class or method if you don't need to inspect the queue    
@patch("tests.testing.tasks.queue", new=TestQueue())
class TestWrapped(IsolatedAsyncioTestCase):
    ...
```

If you need to test something that uses {py:class}`saq.queue.Queue.apply` or {py:class}`saq.queue.Queue.map` you need to pass in the worker settings:
```py
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from saq.testing import TestQueue
from tests.testing.tasks import applies_a_job, settings

@patch("tests.testing.tasks.queue", new=TestQueue(settings))
class TestApplies(IsolatedAsyncioTestCase):
    async def test_apply(self) -> None:
        self.assertEqual(
            await applies_a_job(),
            8
        )
        # Task "add" called directly
```

## Caveats

It's important to patch the entire path of where you queue object is.
If you want to call on the queue directly, {py:class}`unittest.mock` has a caveat that if you 


## Provided assertions

```{eval-rst}
.. autoapiclass:: saq.testing.TestQueue
    :noindex:
    :members: getEnqueued, assertEnqueuedTimes, assertNotEnqueued, getRetried, assertRetriedTimes, assertNotRetried
```
