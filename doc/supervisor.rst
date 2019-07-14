Task supervisor
***************

Task supervisor is a component which manages task thread pool and run task
:doc:`schedulers (workers)<workers>`.

Usage
=====

When **atasker** package is imported, default task supervisor is automatically
created.

.. code:: python

    from atasker import task_supervisor

    task_supervisor.set_config(pool_size=20, reserve_normal=5, reserve_high=5)
    task_supervisor.start()

.. _priorities:

Task priorities
===============

Pool supervisor supports 4 task priorities:

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

.. note::

    pool size can be changed while task supervisor is running.

Blocking
========

Task supervisor is started in its own thread. If you want to block current
thread, you may use method

.. code:: python

    task_supervisor.block()

which will just sleep until task supervisor is active.

Stopping task supervisor
========================

.. code:: python

    task_supervisor.stop(wait=True, stop_schedulers=True, cancel_tasks=False)

Params:

* **wait** wait until tasks and scheduler coroutines finish. If **wait=int**,
  task supervisor will wait until coroutines finish for the max. *wait*
  seconds. However if requested to stop schedulers (workers) or task threads
  are currently running, method *stop* wait until they finish for the unlimited
  time.

* **stop_schedulers** before stopping the main event loop, task scheduler will
  call *stop* method of all schedulers running.

* **cancel_tasks** if specified, task supervisor will try to forcibly cancel
  all scheduler coroutines. 

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