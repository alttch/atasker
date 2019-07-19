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

    mytask()

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

    background_task(mytask, name='mytask', priority=TASK_LOW)()

Manual wrapping supports params:

* **group** put task thread in the specified group
* **name** set task thread name
* **daemon** if *True*, task thread will be launched as daemon.
* **priority** task :ref:`priority<priorities>`

Multiprocessing task
====================

Run as background task
----------------------

To put task into :ref:`multiprocessing pool<create_mp_pool>`, append parameter
*tt=TT_MP*:

.. code:: python

    from atasker import TASK_HIGH, TT_MP

    background_task(
        tests.mp.test, priority=TASK_HIGH, tt=TT_MP)(1,2,3)(x=2)

Optional parameter *callback* can be used to specify function which handles
task result.

Run in async way
----------------

.. code:: python

    from atasker import co_apply, TASK_HIGH

    async def f1():
        result = await co_apply(
            tests.mp.test, args=(1,2,3), kwargs={'x': 2},
            priority=TASK_HIGH)

