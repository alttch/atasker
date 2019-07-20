Task collections
****************

Task collections are useful when you need to run a pack of tasks e.g. on
program startup or shutdown. Currently collections support running task
functions only either in background or as a threads.

Function priority can be specified either as *TASK_\** (e.g. *TASK_NORMAL*) or
as a number (lower = higher priority).

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

Collection constructor supports following arguments:

* **on_error** function launched when function in collection raises an
  exception (as *on_error(e)*).
* **on_error_kwargs** additional kwargs for *on_error* function.
* **include_exceptions** include exceptions into final result dict.

Collection has two method to launch stored functions:

* **run** returns result dict as *{ '<function>': '<function_return>',... }*

* **execute** returns a tuple: *( result, all_ok )*, where all_ok is *True* if
  no function raised an exception.

TaskCollection
==============

Same as function collection, but stored functions are started as tasks in
threads.

Constructor supports additional arguments:

* **supervisor** custom task supervisor
* **poll_delay** custom poll delay

Method *execute()* returns result when all tasks in collection are finished.

