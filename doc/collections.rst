Task collections
****************

Task collections are useful when you need to run a pack of tasks e.g. on
program startup or shutdown. Currently collections support running task
functions only either in a foreground (one-by-one) or as the threads.

Function priority can be specified either as *TASK_\** (e.g. *TASK_NORMAL*) or
as a number (lower = higher priority).

.. automodule:: atasker

FunctionCollection
==================

Simple collection of functions.

.. code:: python

    from atasker import FunctionCollection, TASK_LOW, TASK_HIGH

    def error(e, **kwargs):
        print(e)

    startup = FunctionCollection(on_error=error)

    @startup
    def f1():
        return 1

    @startup(priority=TASK_HIGH)
    def f2():
        return 2

    @startup(priority=TASK_LOW)
    def f3():
        return 3

    result, all_ok = startup.execute()

.. autoclass:: FunctionCollection
    :members:

TaskCollection
==============

Same as function collection, but stored functions are started as tasks in
threads.

Methods *execute()* and *run()* return result when all tasks in collection are
finished.

.. autoclass:: TaskCollection
    :inherited-members:
    :members:
    :show-inheritance:
