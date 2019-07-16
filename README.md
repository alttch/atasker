# atasker
Python library for modern thread pooling and task processing using asyncio.

<img src="https://img.shields.io/badge/license-Apache%202-blue.svg" />
<img src="https://img.shields.io/badge/python-3.5%20%7C%203.6%20%7C%203.7-blue.svg" />

No matter how your code is written, atasker automatically detects blocking
functions and coroutines and launches them in a proper way, in a thread,
asynchronous loop or in multiprocessing pool.

## Install

```bash
pip3 install atasker
```

Sources: https://github.com/alttch/atasker

Documentation: https://atasker.readthedocs.io/

## Why

* asynchronous programming is a perfect way to make your code fast and reliable

* multithreading programming is a perfect way to run blocking code in the
  background

**atasker** combines advantages of both ways: atasker tasks run in separate
threads however task supervisor and workers are completely asynchronous. But
all their public methods are thread-safe.

## Why not standard Python thread pool?

* threads in a standard pool don't have priorities
* workers

## Why not standard asyncio loops?

* compatibility with blocking functions
* async workers

## Code examples

### Start/stop

```python

from atasker import task_supervisor

# set pool size
task_supervisor.set_thread_pool(pool_size=20, reserve_normal=5, reserve_high=5)
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

```python
from atasker import background task, TASK_LOW, TASK_HIGH

# with annotation
@background_task
def mytask():
    print('I am working in the background!')

mytask()

# with manual decoration
def mytask2():
    print('I am working in the background too!')

background_task(mytask2, priority=TASK_HIGH)()
```

### Worker examples

```python
from atasker import background_worker, TASK_HIGH

@background_worker
def worker1(**kwargs):
    print('I am a simple background worker')

@background_worker
async def worker_async(**kwargs):
    print('I am async background worker')

@background_worker(interval=1)
def worker2(**kwargs):
    print('I run every second!')

@background_worker(queue=True)
def worker3(task, **kwargs):
    print('I run when there is a task in my queue')

@background_worker(event=True, priority=TASK_HIGH)
def worker4(**kwargs):
    print('I run when triggered with high priority')

worker1.start()
worker_async.start()
worker2.start()
worker3.start()
worker4.start()

worker3.put('todo1')
worker4.trigger()

from atasker import BackgroundIntervalWorker

class MyWorker(BackgroundIntervalWorker):

    def run(self, **kwargs):
        print('I am custom worker class')

worker5 = MyWorker(interval=0.1, name='worker5')
worker5.start()
```
