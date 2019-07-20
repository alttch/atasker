Thread local proxy
******************

Simple proxy for *threading.local* namespace.

.. code:: python

    from atasker import g

    if not g.has('db'):
        g.set('db', <new_db_connection>)

Supports methods:

.. code:: python

    # returns attr value or default value if specified
    g.get(attr, default=None)

    # returns True if attribute exists
    g.has(attr)

    # set attribute value
    g.set(attr, value)

    # delete attribute
    g.clear(attr)
