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

    @background_task
    def mytask():
        print('I am working in the background!')

    background_task(mytask, name='mytask', priority=TASK_LOW)()

Manual wrapping supports params:

* **group** put task thread in the specified group
* **name** set task thread name
* **daemon** if *True*, task thread will be launched as daemon.
* **priority** task :ref:`priority<priorities>`

