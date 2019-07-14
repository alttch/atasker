# atasker
Python library for modern thread polling and task processing using asyncio

## Why

* asynchronous programming is a perfect way to make your code fast and reliable

* multithreading programming is a perfect way to run blocking code in the
  background

**atasker** combines advantages of both ways: atasker tasks run in separate
threads however task supervisor and workers are completely asynchronous and all
their methods are thread-safe.

## Why not standard Python thread pool?

The answer is simple:

* threads in a standard pool don't have priorities
* workers

## Code examples

### Start/stop

```python

from atasker import task_supervisor

# set pool size
task_supervisor.set_config(pool_size=20, reserve_normal=5, reserve_high=5)
task_supervisor.start()
# ...
# start workers, other threads etc.
# ...
# optionally block current thread
task_supervisor.block()

# stop from any thread
task_supervisor.stop()
```

### Background task

from atasker import background task, TASK_LOW, TASK_HIGH

```python
# with annotation
@background_task(priority=TASK_LOW)
def mytask():
    print('I am working in the background!')

mytask()

# for manual decoration
def mytask2():
    print('I am working in the background too!')

background_task(mytask2, priority=TASK_HIGH)()
```

### Workers

* **BackgroundWorker** - worker loop starts in a separated thread and loops
worker function until stopped
