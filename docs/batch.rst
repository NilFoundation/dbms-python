Batch API Execution
-------------------

In **batch API executions**, requests to DbmsDB server are stored in client-side
in-memory queue, and committed together in a single HTTP call. After the commit,
results can be retrieved later from :ref:`BatchJob` objects.

**Example:**

.. code-block:: python

    from dbms import DbmsClient, SQLQueryExecuteError

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Get the API wrapper for "students" relation.
    students = db.relation('students')

    # Begin batch execution via context manager. This returns an instance of
    # BatchDatabase, a database-level API wrapper tailored specifically for
    # batch execution. The batch is automatically committed when exiting the
    # context. The BatchDatabase wrapper cannot be reused after commit.
    with db.begin_batch_execution(return_result=True) as batch_db:

        # Child wrappers are also tailored for batch execution.
        batch_sql = batch_db.sql
        batch_col = batch_db.relation('students')

        # API execution context is always set to "batch".
        assert batch_db.context == 'batch'
        assert batch_sql.context == 'batch'
        assert batch_col.context == 'batch'

        # BatchJob objects are returned instead of results.
        job1 = batch_col.insert({'_key': 'Kris'})
        job2 = batch_col.insert({'_key': 'Rita'})
        job3 = batch_sql.execute('RETURN 100000')
        job4 = batch_sql.execute('INVALID QUERY')  # Fails due to syntax error.

    # Upon exiting context, batch is automatically committed.
    assert 'Kris' in students
    assert 'Rita' in students

    # Retrieve the status of each batch job.
    for job in batch_db.queued_jobs():
        # Status is set to either "pending" (transaction is not committed yet
        # and result is not available) or "done" (transaction is committed and
        # result is available).
        assert job.status() in {'pending', 'done'}

    # Retrieve the results of successful jobs.
    metadata = job1.result()
    assert metadata['_id'] == 'students/Kris'

    metadata = job2.result()
    assert metadata['_id'] == 'students/Rita'

    cursor = job3.result()
    assert cursor.next() == 100000

    # If a job fails, the exception is propagated up during result retrieval.
    try:
        result = job4.result()
    except SQLQueryExecuteError as err:
        assert err.http_code == 400
        assert err.error_code == 1501
        assert 'syntax error' in err.message

    # Batch execution can be initiated without using a context manager.
    # If return_result parameter is set to False, no jobs are returned.
    batch_db = db.begin_batch_execution(return_result=False)
    batch_db.relation('students').insert({'_key': 'Jake'})
    batch_db.relation('students').insert({'_key': 'Jill'})

    # The commit must be called explicitly.
    batch_db.commit()
    assert 'Jake' in students
    assert 'Jill' in students

.. note::
    * Be mindful of client-side memory capacity when issuing a large number of
      requests in single batch execution.
    * :ref:`BatchDatabase` and :ref:`BatchJob` instances are stateful objects,
      and should not be shared across multiple threads.
    * :ref:`BatchDatabase` instance cannot be reused after commit.

See :ref:`BatchDatabase` and :ref:`BatchJob` for API specification.
