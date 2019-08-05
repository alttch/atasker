Locker helper/decorator
***********************

.. code:: python

    from atasker import Locker

    def critical_exception():
        # do something, e.g. restart/kill myself
        import os, signal
        os.kill(os.getpid(), signal.SIGKILL)

    with_lock1 = Locker(mod='main', timeout=5)
    with_lock1.critical = critical_exception

    @with_lock1
    def test():
        # thread-safe access to resources locked with with_lock1


Supports methods:

.. automodule:: atasker
.. autoclass:: Locker
    :members:
