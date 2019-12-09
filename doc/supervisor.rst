Task supervisor
***************

Task supervisor is a component which manages task thread pool and run task
:doc:`schedulers (workers)<workers>`.

.. contents::

Usage
=====

When **atasker** package is imported, default task supervisor is automatically
created.

.. code:: python

    from atasker import task_supervisor

    # thread pool
    task_supervisor.set_thread_pool(
        pool_size=20, reserve_normal=5, reserve_high=5)
    task_supervisor.start()

.. warning::

    Task supervisor must be started before any scheduler/worker or task.

.. _priorities:

Task priorities
===============

Task supervisor supports 4 task priorities:

* TASK_LOW
* TASK_NORMAL (default)
* TASK_HIGH
* TASK_CRITICAL

.. code:: python

    from atasker import TASK_HIGH

    def test():
        pass

    background_task(test, name='test', priority=TASK_HIGH)()

Pool size
=========

Parameter **pool_size** for **task_supervisor.set_thread_pool** defines size of
the task (thread) pool.

Pool size means the maximum number of the concurrent tasks which can run. If
task supervisor receive more tasks than pool size has, they will wait until
some running task is finished.

Actually, parameter **pool_size** defines pool size for the tasks, started with
*TASK_LOW* priority. Tasks with higher priority have "reserves": *pool_size=20,
reserve_normal=5* means create pool for 20 tasks but reserve 5 more places for
the tasks with *TASK_NORMAL* priority. In this example, when task supervisor
receives such task, pool is "extended", up to 5 places.

For *TASK_HIGH* pool size can be extended up to *pool_size + reserve_normal +
reserve_high*, so in the example above: *20 + 5 + 5 = 30*.

Tasks with priority *TASK_CRITICAL* are always started instantly, no matter how
busy task pool is, and thread pool is being extended for them with no limits.
Multiprocessing critical tasks are started as soon as *multiprocessing.Pool*
object has free space for the task.

To make pool size unlimited, set *pool_size=0*.

Parameters *min_size* and *max_size* set actual system thread pool size. If
*max_size* is not specified, it's set to *pool_size + reserve_normal +
reserve_high*. It's recommended to set *max_size* slightly larger manually to
have a space for critical tasks.

By default, *max_size* is CPU count * 5. You may use argument *min_size='max'*
to automatically set minimal pool size to max.

.. note::

    pool size can be changed while task supervisor is running.

Poll delay
==========

Poll delay is a delay (in seconds), which is used by task queue manager, in
:doc:`workers<workers>` and some other methods like *start/stop*.

Lower poll delay = higher CPU usage, higher poll delay = faster reaction time.

Default poll delay is 0.1 second. Can be changed with:

.. code:: python

    task_supervisor.poll_delay = 0.01 # set poll delay to 10ms

Blocking
========

Task supervisor is started in its own thread. If you want to block current
thread, you may use method

.. code:: python

    task_supervisor.block()

which will just sleep while task supervisor is active.

Timeouts
========

Task supervisor can log timeouts (when task isn't launched within a specified
number of seconds) and run timeout handler functions:

.. code:: python

    def warning(t):
        # t = task thread object
        print('Task thread {} is not launched yet'.format(t))

    def critical(t):
        print('All is worse than expected')

    task_supervisor.timeout_warning = 5
    task_supervisor.timeout_warning_func = warn
    task_supervisor.timeout_critical = 10
    task_supervisor.timeout_critical_func = critical

Stopping task supervisor
========================

.. code:: python

    task_supervisor.stop(wait=True, stop_schedulers=True, cancel_tasks=False)

Params:

* **wait** wait until tasks and scheduler coroutines finish. If
  **wait=<number>**, task supervisor will wait until coroutines finish for the
  max. *wait* seconds. However if requested to stop schedulers (workers) or
  task threads are currently running, method *stop* wait until they finish for
  the unlimited time.

* **stop_schedulers** before stopping the main event loop, task scheduler will
  call *stop* method of all schedulers running.

* **cancel_tasks** if specified, task supervisor will try to forcibly cancel
  all scheduler coroutines. 

.. _aloops:

aloops: async executors and tasks
=================================

Usually it's unsafe to run both :doc:`schedulers (workers)<workers>` executors
and custom tasks in supervisor's event loop. Workers use event loop by default
and if anything is blocked, the program may be freezed.

To avoid this, it's strongly recommended to create independent async loops for
your custom tasks. atasker supervisor has built-in engine for async loops,
called "aloops", each aloop run in a separated thread and doesn't interfere
with supervisor event loop and others.

Create
------

If you plan to use async worker executors, create aloop:

.. code:: python

   a = task_supervisor.create_aloop('myworkers', default=True, daemon=True)
   # the loop is instantly started by default, to prevent add param start=False
   # and then use
   # task_supervisor.start_aloop('myworkers')

To determine in which thread executor is started, simply get its name. aloop
threads are called "supervisor_aloop_<name>".

Using with workers
------------------

Workers automatically launch async executor function in default aloop, or aloop
can be specified with *loop=* at init or *_loop=* at startup.

Executing own coroutines
------------------------

aloops have 2 methods to execute own coroutines:

.. code:: python

   # put coroutine to loop
   task = aloop.background_task(coro(args))

   # blocking wait for result from coroutine
   result = aloop.run(coro(args))

Other supervisor methods
------------------------

.. note::

   It's not recommended to create/start/stop aloops without supervisor

.. code:: python

   # set default aloop
   task_supervisor.set_default_aloop(aloop):

   # get aloop by name
   task_supervisor.get_aloop(name)

   # stop aloop (not required, supervisor stops all aloops at shutdown)
   task_supervisor.stop_aloop(name)

   # get aloop async event loop object for direct access
   aloop.get_loop()

.. _create_mp_pool:

Multiprocessing
===============

Multiprocessing pool may be used by workers and background tasks to execute a
part of code.

To create multiprocessing pool, use method:

.. code:: python

    from atasker import task_supervisor

    # task_supervisor.create_mp_pool(<args for multiprocessing.Pool>)
    # e.g.
    task_supervisor.create_mp_pool(processes=8)

    # use custom mp Pool

    from multiprocessing import Pool

    pool = Pool(processes=4)
    task_supervisor.mp_pool = pool

    # set mp pool size. if pool wasn't created before, it will be initialized
    # with processes=(pool_size+reserve_normal+reserve_high)
    task_supervisor.set_mp_pool(
        pool_size=20, reserve_normal=5, reserve_high=5)

Custom task supervisor
======================

.. code:: python

    from atasker import TaskSupervisor

    my_supervisor = TaskSupervisor(
        pool_size=100, reserve_normal=10, reserve_high=10)

    class MyTaskSupervisor(TaskSupervisor):
        # .......

    my_supervisor2 = MyTaskSupervisor()

Putting own tasks
=================

If you can not use :doc:`background tasks<tasks>` for some reason, you may
put own tasks manually and put it to task supervisor to launch:

.. code:: python

    task = task_supervisor.put_task(target=myfunc, args=(), kwargs={},
      priority=TASK_NORMAL, delay=None)

If *delay* is specified, the thread is started after the corresponding delay
(seconds).

After the function thread is finished, it should notify task supervisor:

.. code:: python

    task_supervisor.mark_task_completed(task=task) # or task_id = task.id

If no *task_id* specified, current thread ID is being used:

.. code:: python

   # note: custom task targets always get _task_id in kwargs
    def mytask(**kwargs):
       # ... perform calculations
      task_supervisor.mark_task_completed(task_id=kwargs['_task_id'])

    task_supervisor.put_task(target=mytask)

.. note::

   If you need to know task id, before task is put (e.g. for task callback),
   you may generate own and call *put_task* with *task_id=task_id* parameter.

Putting own tasks in multiprocessing pool
=========================================

To put own task into multiprocessing pool, you must create tuple object which
contains:

* unique task id
* task function (static method)
* function args
* function kwargs
* result callback function

.. code:: python

    import uuid

    from atasker import TT_MP

    task = task_supervisor.put_task(
       target=<somemodule.staticmethod>, callback=<somefunc>, tt=TT_MP)

After the function is finished, you should notify task supervisor:

.. code:: python

    task_supervisor.mark_task_completed(task_id=<task_id>, tt=TT_MP)

Creating own schedulers
=======================

Own task scheduler (worker) can be registered in task supervisor with:

.. code:: python

    task_supervisor.register_scheduler(scheduler)

Where *scheduler* = scheduler object, which should implement at least *stop*
(regular) and *loop* (async) methods.

Task supervisor can also register synchronous schedulers/workers, but it can
only stop them when *stop* method is called:

.. code:: python

    task_supervisor.register_sync_scheduler(scheduler)

To unregister schedulers from task supervisor, use *unregister_scheduler* and
*unregister_sync_scheduler* methods.
