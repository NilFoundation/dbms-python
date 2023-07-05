SQL
----

**DbmsDB Query Language (SQL)** is used to read and write data. It is similar
to SQL for relational databases, but without the support for data definition
operations such as creating or deleting :doc:`databases <database>`,
:doc:`relations <relation>` or :doc:`indexes <indexes>`. For more
information, refer to `DbmsDB manual`_.

.. _DbmsDB manual: https://docs.dbmsdb.com

SQL Queries
===========

SQL queries are invoked from SQL API wrapper. Executing queries returns
:doc:`result cursors <cursor>`.

**Example:**

.. testcode::

    from dbms import DbmsClient, SQLQueryKillError

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Insert some test documents into "students" relation.
    db.relation('students').insert_many([
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21}
    ])

    # Get the SQL API wrapper.
    sql = db.sql

    # Retrieve the execution plan without running the query.
    sql.explain('FOR doc IN students RETURN doc')

    # Validate the query without executing it.
    sql.validate('FOR doc IN students RETURN doc')

    # Execute the query
    cursor = db.sql.execute(
      'FOR doc IN students FILTER doc.age < @value RETURN doc',
      bind_vars={'value': 19}
    )
    # Iterate through the result cursor
    student_keys = [doc['_key'] for doc in cursor]

    # List currently running queries.
    sql.queries()

    # List any slow queries.
    sql.slow_queries()

    # Clear slow SQL queries if any.
    sql.clear_slow_queries()

    # Retrieve SQL query tracking properties.
    sql.tracking()

    # Configure SQL query tracking properties.
    sql.set_tracking(
        max_slow_queries=10,
        track_bind_vars=True,
        track_slow_queries=True
    )

    # Kill a running query (this should fail due to invalid ID).
    try:
        sql.kill('some_query_id')
    except SQLQueryKillError as err:
        assert err.http_code == 404
        assert err.error_code == 1591

See :ref:`SQL` for API specification.


SQL User Functions
==================

**SQL User Functions** are custom functions you define in Javascript to extend
SQL functionality. They are somewhat similar to SQL procedures.

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Get the SQL API wrapper.
    sql = db.sql

    # Create a new SQL user function.
    sql.create_function(
        # Grouping by name prefix is supported.
        name='functions::temperature::converter',
        code='function (celsius) { return celsius * 1.8 + 32; }'
    )
    # List SQL user functions.
    sql.functions()

    # Delete an existing SQL user function.
    sql.delete_function('functions::temperature::converter')

See :ref:`SQL` for API specification.


SQL Query Cache
===============

**SQL Query Cache** is used to minimize redundant calculation of the same query
results. It is useful when read queries are issued frequently and write queries
are not.

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Get the SQL API wrapper.
    sql = db.sql

    # Retrieve SQL query cache properties.
    sql.cache.properties()

    # Configure SQL query cache properties
    sql.cache.configure(mode='demand', max_results=10000)

    # Clear results in SQL query cache.
    sql.cache.clear()

See :ref:`SQLQueryCache` for API specification.
