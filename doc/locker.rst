Locker helper/decorator
***********************

.. code:: python

    from atasker import Locker

    def critical_exception():
        # do something, e.g. restart/kill myself
        import os, signal
        os.kill(os.getpid(), signal.SIGKILL)

    lock1 = Locker(mod='main', timeout=5)
    lock1.critical = critical_exception

    # use as decorator

    @with_lock1
    def test():
        # thread-safe access to resources locked with lock1

    # with
    with lock1:
        # thread-safe access to resources locked with lock1


Supports methods:

.. automodule:: atasker
.. autoclass:: Locker
    :members:
