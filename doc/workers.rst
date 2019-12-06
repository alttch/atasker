Workers
*******

Worker is an object which runs specified function (executor) in a loop.

.. contents::

Common
======

Worker parameters
-----------------

All workers support the following initial parameters:

* **name** worker name (default: name of executor function if specified,
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

Methods
-------

.. automodule:: atasker
.. autoclass:: BackgroundWorker
    :members:

Overriding parameters at startup
--------------------------------

Initial parameters *name*, *priority* and *o* can be overriden during
worker startup (first two - as *_name* and *_priority*)

.. code:: python

    myworker.start(_name='worker1', _priority=atasker.TASK_LOW)

Executor function
-----------------

Worker executor function is either specified with annotation or named *run*
(see examples below). The function should always have *\*\*kwargs* param.

Executor function gets in args/kwargs:

* all parameters *worker.start* has been started with.

* **_worker** current worker object
* **_name** current worker name
* **_task_id** if executor function is started in multiprocessing pool - ID of
  current task (for thread pool, task id = thread name).

.. note::

    If executor function return *False*, worker stops itself.

Asynchronous executor function
------------------------------

Executor function can be asynchronous, in this case it's executed inside
:doc:`task supervisor<supervisor>` loop, no new thread is started and
*priority* is ignored.

When *background_worker* decorator detects asynchronous function, class
*BackgroundAsyncWorker* is automatically used instead of *BackgroundWorker*
(*BackgroundQueueWorker*, *BackgroundEventWorker* and
*BackgroundIntervalWorker* support synchronous functions out-of-the-box).

Additional worker parameter *loop* (*_loop* at startup) may be specified to put
executor function inside external async loop.

.. note::

   To prevent interference between supervisor event loop and executors, it's
   strongly recommended to specify own async event loop or create
   :ref:`aloop<aloops>`.

Multiprocessing executor function
---------------------------------

To use multiprocessing, :ref:`task supervisor<create_mp_pool>` mp pool must be
created.

If executor method *run* is defined as static, workers automatically detect
this and use multiprocessing pool of task supervisor to launch executor.

.. note::

    As executor is started in separate process, it doesn't have an access to
    *self* object.

Additionally, method *process_result* must be defined in worker class to
process executor result. The method can stop worker by returning *False* value.

Example, let's define *BackgroundQueueWorker*. Python multiprocessing module
can not pick execution function defined via annotation, so worker class is
required. Create it in separate module as Python multiprocessing can not pick
methods from the module where the worker is started:

.. warning::

    Multiprocessing executor function should always finish correctly, without
    any exceptions otherwise callback function is never called and task become
    "freezed" in pool.

*myworker.py*

.. code:: python

    class MyWorker(BackgroundQueueWorker):

        # executed in another process via task_supervisor
        @staticmethod
        def run(task, *args, **kwargs):
            # .. process task
            return '<task result>'

        def process_result(self, result):
            # process result

*main.py*

.. code:: python

    from myworker import MyWorker

    worker = MyWorker()
    worker.start()
    # .....
    worker.put('task')
    # .....
    worker.stop()

Workers
=======

BackgroundWorker
----------------

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

BackgroundAsyncWorker
---------------------

Similar to *BackgroundWorker* but used for async executor functions. Has
additional parameter *loop=* (*_loop* in start function) to specify either
async event loop or :ref:`aloop<aloops>` object. By default either task
supervisor event loop or task supervisor default aloop is used.

.. code:: python

    # with annotation - function becomes worker executor
    from atasker import background_worker

    @background_worker
    async def async_worker(**kwargs):
        print('I am async worker')

    async_worker.start()

    # with class 
    from atasker import BackgroundAsyncWorker

    class MyWorker(BackgroundAsyncWorker):

        async def run(self, *args, **kwargs):
            print('I am async worker too')

    worker = MyWorker()
    worker.start()

BackgroundQueueWorker
---------------------

Background worker which gets data from asynchronous queue and passes it to
synchronous or Asynchronous executor.

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
---------------------

Background worker which runs asynchronous loop waiting for the event and
launches synchronous or asynchronous executor when it's happened.

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

BackgroundIntervalWorker
------------------------

Background worker which runs synchronous or asynchronous executor function with
the specified interval or delay.

Worker initial parameters:

* **interval** run executor with a specified interval (in seconds)
* **delay** delay *between* executor launches
* **delay_before** delay *before* executor launch

Parameters *interval* and *delay* can not be used together. All parameters can
be overriden during startup by adding *_* prefix (e.g.
*worker.start(_interval=1)*)

Background interval worker is created automatically, as soon as annotator
detects one of the parameters above:

.. code:: python

    @background_worker(interval=1)
    def myfunc(**kwargs):
        print('I run every second!')

    @background_worker(interval=1)
    async def myfunc2(**kwargs):
        print('I run every second and I am async!')

    myfunc.start()
    myfunc2.start()

As well as event worker, **BackgroundIntervalWorker** supports manual executor
triggering with *worker.trigger()*

