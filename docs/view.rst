Views and DbmsSearch
----------------------

Python-dbms supports **view** management. For more information on view
properties, refer to `DbmsDB manual`_.

.. _DbmsDB manual: https://docs.dbmsdb.com

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Retrieve list of views.
    db.views()

    # Create a view.
    db.create_view(
        name='foo',
        view_type='dbmssearch',
        properties={
            'cleanupIntervalStep': 0,
            'consolidationIntervalMsec': 0
        }
    )

    # Rename a view.
    db.rename_view('foo', 'bar')

    # Retrieve view properties.
    db.view('bar')

    # Partially update view properties.
    db.update_view(
        name='bar',
        properties={
            'cleanupIntervalStep': 1000,
            'consolidationIntervalMsec': 200
        }
    )

    # Replace view properties. Unspecified ones are reset to default.
    db.replace_view(
        name='bar',
        properties={'cleanupIntervalStep': 2000}
    )

    # Delete a view.
    db.delete_view('bar')


Python-dbms also supports **DbmsSearch** views.

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Create an DbmsSearch view.
    db.create_dbmssearch_view(
        name='dbmssearch_view',
        properties={'cleanupIntervalStep': 0}
    )

    # Partially update an DbmsSearch view.
    db.update_dbmssearch_view(
        name='dbmssearch_view',
        properties={'cleanupIntervalStep': 1000}
    )

    # Replace an DbmsSearch view.
    db.replace_dbmssearch_view(
        name='dbmssearch_view',
        properties={'cleanupIntervalStep': 2000}
    )

    # DbmsSearch views can be retrieved or deleted using regular view API
    db.view('dbmssearch_view')
    db.delete_view('dbmssearch_view')


For more information on the content of view **properties**, see
https://www.dbmsdb.com/docs/stable/http/views-dbmssearch.html

Refer to :ref:`StandardDatabase` class for API specification.
