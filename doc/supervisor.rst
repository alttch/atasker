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

    task_supervisor.set_config(pool_size=20, reserve_normal=5, reserve_high=5)
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

    background_task(test, name='test', wait_start=True, priority=TASK_HIGH)()

Pool size
=========

Parameter **pool_size** for **task_supervisor.set_config** (or supervisor
constructor) defines size of the task (thread) pool.

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
busy task pool is, and the pool is being extended for them with no limits.

To make pool size unlimited, set *pool_size=0*.

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

which will just sleep until task supervisor is active.

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

Custom task supervisor
======================

.. code:: python

    from atasker import TaskSupervisor

    my_supervisor = TaskSupervisor(
        pool_size=100, reserve_normal=10, reserve_high=10)

    class MyTaskSupervisor(TaskSupervisor):
        # .......

    my_supervisor2 = MyTaskSupervisor()

Putting own threads
===================

If you can not use :doc:`background tasks<tasks>` for some reason, You may
create *threading.Thread* object manually and put it to task supervisor to
launch:

.. code:: python

    t = threading.Thread(target=myfunc)
    task_supervisor.put_task(t, priority=TASK_NORMAL, delay=None)

If *delay* is specified, the thread is started after the corresponding delay
(seconds).

After the function thread is finished, it should notify task supervisor:

.. code:: python

    task_supervisor.mark_task_completed(task=None)

Where *task* - thread object which is finished. If no object specified, current
thread ID is being used:

.. code:: python

    def mytask():
       # ... perform calculations
      task_supervisor.mark_task_completed() 

    t = threading.Thread(target=mytask)
    task_supervisor.put_task(t)

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
