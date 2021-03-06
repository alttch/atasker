atasker
=======

Python library for modern thread / multiprocessing pooling and task
processing via asyncio.

No matter how your code is written, atasker automatically detects
blocking functions and coroutines and launches them in a proper way, in
a thread, asynchronous loop or in multiprocessing pool.

Tasks are grouped into pools. If there’s no space in pool, task is being
placed into waiting queue according to their priority. Pool also has
“reserve” for the tasks with priorities “normal” and higher. Tasks with
“critical” priority are always executed instantly.

This library is useful if you have a project with many similar tasks
which produce approximately equal CPU/memory load, e.g. API responses,
scheduled resource state updates etc.

Install
-------

.. code:: bash

   pip3 install atasker

Sources: https://github.com/alttch/atasker

Documentation: https://atasker.readthedocs.io/

Why
---

-  asynchronous programming is a perfect way to make your code fast and
   reliable

-  multithreading programming is a perfect way to run blocking code in
   the background

**atasker** combines advantages of both ways: atasker tasks run in
separate threads however task supervisor and workers are completely
asynchronous. But all their public methods are thread-safe.

Why not standard Python thread pool?
------------------------------------

-  threads in a standard pool don’t have priorities
-  workers

Why not standard asyncio loops?
-------------------------------

-  compatibility with blocking functions
-  async workers

Why not concurrent.futures?
---------------------------

**concurrent.futures** is a great standard Python library which allows
you to execute specified tasks in a pool of workers.

**atasker** method *background_task* solves the same problem but in
slightly different way, adding priorities to the tasks, while *atasker*
workers do absolutely different job:

-  in *concurrent.futures* worker is a pool member which executes the
   single specified task.

-  in *atasker* worker is an object, which continuously *generates* new
   tasks with the specified interval or on external event, and executes
   them in thread or multiprocessing pool.

Code examples
-------------

Start/stop
~~~~~~~~~~

.. code:: python


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

Background task
~~~~~~~~~~~~~~~

.. code:: python

   from atasker import background_task, TASK_LOW, TASK_HIGH, wait_completed

   # with annotation
   @background_task
   def mytask():
       print('I am working in the background!')
       return 777

   task = mytask()

   # optional
   result = wait_completed(task)

   print(task.result) # 777
   print(result) # 777

   # with manual decoration
   def mytask2():
       print('I am working in the background too!')

   task = background_task(mytask2, priority=TASK_HIGH)()

Async tasks
~~~~~~~~~~~

.. code:: python

   # new asyncio loop is automatically created in own thread
   a1 = task_supervisor.create_aloop('myaloop', default=True)

   async def calc(a):
       print(a)
       await asyncio.sleep(1)
       print(a * 2)
       return a * 3

   # call from sync code

   # put coroutine
   task = background_task(calc)(1)

   wait_completed(task)

   # run coroutine and wait for result
   result = a1.run(calc(1))

Worker examples
~~~~~~~~~~~~~~~

.. code:: python

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
