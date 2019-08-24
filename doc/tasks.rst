Tasks
*****

Task is a Python function which will be launched in the separate thread.

Defining task with annotation
=============================

.. code:: python

    from atasker import background_task

    @background_task
    def mytask():
        print('I am working in the background!')

    task_id = mytask()

It's not required to notify task supervisor about task completion,
*background_task* will do this automatically as soon as task function is
finished.

All start parameters (args, kwargs) are passed to task functions as-is.

Task function without annotation
================================

To start task function without annotation, you must manually decorate it:

.. code:: python

    from atasker import background_task, TASK_LOW

    def mytask():
        print('I am working in the background!')

    task_id = background_task(mytask, name='mytask', priority=TASK_LOW)()

.. automodule:: atasker
.. autofunction:: background_task

Multiprocessing task
====================

Run as background task
----------------------

To put task into :ref:`multiprocessing pool<create_mp_pool>`, append parameter
*tt=TT_MP*:

.. code:: python

    from atasker import TASK_HIGH, TT_MP

    task_id = background_task(
        tests.mp.test, priority=TASK_HIGH, tt=TT_MP)(1, 2, 3, x=2)

Optional parameter *callback* can be used to specify function which handles
task result.

.. note::

   Multiprocessing target function always receives *_task_id* param.

Run in async way
----------------

You may put task from your coroutine, without using callback, example:

.. code:: python

    from atasker import co_mp_apply, TASK_HIGH

    async def f1():
        result = await co_mp_apply(
            tests.mp.test, args=(1,2,3), kwargs={'x': 2},
            priority=TASK_HIGH)

.. autofunction:: co_mp_apply

Task info
=========

Task id can later be used to obtain task info:

.. code:: python

   from atasker import task_supervisor

   task_info = task_supervisor.get_task_info(task_id)

Task info object fields:

* **id** task id
* **task** task object
* **tt** task type (TT_THREAD, TT_MP)
* **priority** task priority
* **time_queued** time when task was queued
* **time_started** time when task was started
* **status** 0 - queued, 1 - started, -1 - canceled

If task info is *None*, consider the task is completed and supervisor destroyed
information about it.
