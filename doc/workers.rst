Workers
*******

Worker is an object which runs specified function (executor) in a loop.

Common
======

Worker parameters
-----------------

All workers support the following initial parameters:

* **worker_name** worker name (default: name of executor function if specified,
  otherwise: auto-generated UUID)

* **func** executor function (default: *worker.run*)

* **daemon** worker executor will run as a daemon

* **priority** worker thread priority

* **o** special object, passed as-is to executor (e.g. object worker is running
  for)

* **on_error** a function which is called as *func(e)*, if executor raises an
  exception

* **on_error_kwargs** kwargs for *on_error* function

* **supervisor** alternative :doc:`task supervisor<supervisor>`

* **poll_delay** worker poll delay (default: task supervisor poll delay)

Overriding parameters at startup
--------------------------------

Initial parameters *worker_name*, *priority* and *o* can be overriden during
worker startup (first two - as *_worker_name* and *_priority*)

.. code:: python

    myworker.start(_worker_name='worker1', _priority=atasker.TASK_LOW)

Executor function
-----------------

Worker executor function is either specified with annotation or named *run*
(see examples below). The function should always have *\*\*kwargs* param.

Executor function gets in args/kwargs:

* all parameters *worker.start* has been started with.

* **_worker** current worker object
* **_worker_name** current worker name

.. note::

    If executor function return *False*, worker stops itself.

.. contents::

BackgroundWorker
================

Background worker is a worker which continuously run executor function in a
loop without any condition. Loop of this worker is synchronous and is started
in separate thread instantly.

.. code:: python

    # with annotation - function becomes worker executor
    from atasker import background_worker

    @background_worker
    def myfunc(*args, **kwargs):
        print('I am background worker')

    # with class 
    from atasker import BackgroundWorker

    class MyWorker(BackgroundWorker):

        def run(self, *args, **kwargs):
            print('I am a worker too')

    myfunc.start()

    myworker2 = MyWorker()
    myworker2.start()

    # ............

    # stop first worker
    myfunc.stop()
    # stop 2nd worker, don't wait until it is really stopped
    myworker2.stop(wait=False)

BackgroundIntervalWorker
========================

Background worker which runs synchronous executor function but has asynchronous
loop.

Worker initial parameters:

* **interval** run executor with a specified interval (in seconds)
* **delay** delay between launches
* **delay_before** delay before executor launch

Parameters *interval* and *delay* can not be used together. All parameters can
be overriden during startup by adding *_* prefix (e.g.
*worker.start(_interval=1)*)

Background interval worker is created automatically, as soon as annotator
detects one of the parameters above:

.. code:: python

    @background_worker(interval=1)
    def myfunc(**kwargs):
        print('I run every second!')

    myfunc.start()

BackgroundQueueWorker
=====================

Background worker which gets data from asynchronous queue and passes it to
synchronous executor.

Queue worker is created as soon as annotator detects *q=True* or *queue=True*
param. Default queue is *asyncio.queues.Queue*. If you want to use e.g.
priority queue, specify its class instead of just *True*.

.. code:: python

    # with annotation - function becomes worker executor
    from atasker import background_worker

    @background_worker(q=True)
    def f(task, **kwargs):
        print('Got task from queue: {}'.format(task))

    @background_worker(q=asyncio.queues.PriorityQueue)
    def f2(task, **kwargs):
        print('Got task from queue too: {}'.format(task))

    # with class 
    from atasker import BackgroundQueueWorker

    class MyWorker(BackgroundQueueWorker):

        def run(self, task, *args, **kwargs):
            print('my task is {}'.format(task))


    f.start()
    f2.start()
    worker3 = MyWorker()
    worker3.start()
    f.put('task 1')
    f2.put('task 2')
    worker3.put('task 3')

**put** method is used to put task into worker's queue. The method is
thread-safe.

BackgroundEventWorker
=====================

Background worker which runs asynchronous loop waiting for the event and
launches synchronous executor when it's happened.

Event worker is created as soon as annotator detects *e=True* or *event=True*
param.

.. code:: python

    # with annotation - function becomes worker executor
    from atasker import background_worker

    @background_worker(e=True)
    def f(task, **kwargs):
        print('happened')

    # with class 
    from atasker import BackgroundEventWorker

    class MyWorker(BackgroundEventWorker):

        def run(self, *args, **kwargs):
            print('happened')


    f.start()
    worker3 = MyWorker()
    worker3.start()
    f.trigger()
    worker3.trigger()

**trigger** method is used to put task into worker's queue. The method is
thread-safe.
