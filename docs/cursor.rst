Cursors
-------

Many operations provided by python-dbms (e.g. executing :doc:`sql` queries)
return result **cursors** to batch the network communication between DbmsDB
server and python-dbms client. Each HTTP request from a cursor fetches the
next batch of results (usually documents). Depending on the query, the total
number of items in the result set may or may not be known in advance.

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Set up some test data to query against.
    db.relation('students').insert_many([
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21},
        {'_key': 'Suzy', 'age': 23},
        {'_key': 'Dave', 'age': 20}
    ])

    # Execute an SQL query which returns a cursor object.
    cursor = db.sql.execute(
        'FOR doc IN students FILTER doc.age > @val RETURN doc',
        bind_vars={'val': 17},
        batch_size=2,
        count=True
    )

    # Get the cursor ID.
    cursor.id

    # Get the items in the current batch.
    cursor.batch()

    # Check if the current batch is empty.
    cursor.empty()

    # Get the total count of the result set.
    cursor.count()

    # Flag indicating if there are more to be fetched from server.
    cursor.has_more()

    # Flag indicating if the results are cached.
    cursor.cached()

    # Get the cursor statistics.
    cursor.statistics()

    # Get the performance profile.
    cursor.profile()

    # Get any warnings produced from the query.
    cursor.warnings()

    # Return the next item from the cursor. If current batch is depleted, the
    # next batch if fetched from the server automatically.
    cursor.next()

    # Return the next item from the cursor. If current batch is depleted, an
    # exception is thrown. You need to fetch the next batch manually.
    cursor.pop()

    # Fetch the next batch and add them to the cursor object.
    cursor.fetch()

    # Delete the cursor from the server.
    cursor.close()

See :ref:`Cursor` for API specification.

If the fetched result batch is depleted while you are iterating over a cursor
(or while calling the method :func:`dbms.cursor.Cursor.next`), python-dbms
automatically sends an HTTP request to the server to fetch the next batch
(just-in-time style). To control exactly when the fetches occur, you can use
methods :func:`dbms.cursor.Cursor.fetch` and :func:`dbms.cursor.Cursor.pop`
instead.

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Set up some test data to query against.
    db.relation('students').insert_many([
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21}
    ])

    # If you iterate over the cursor or call cursor.next(), batches are
    # fetched automatically from the server just-in-time style.
    cursor = db.sql.execute('FOR doc IN students RETURN doc', batch_size=1)
    result = [doc for doc in cursor]

    # Alternatively, you can manually fetch and pop for finer control.
    cursor = db.sql.execute('FOR doc IN students RETURN doc', batch_size=1)
    while cursor.has_more(): # Fetch until nothing is left on the server.
        cursor.fetch()
    while not cursor.empty(): # Pop until nothing is left on the cursor.
        cursor.pop()
