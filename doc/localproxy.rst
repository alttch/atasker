Thread local proxy
******************

.. code:: python

    from atasker import g

    if not g.has('db'):
        g.set('db', <new_db_connection>)

Supports methods:

.. automodule:: atasker
.. autoclass:: LocalProxy
    :members:
