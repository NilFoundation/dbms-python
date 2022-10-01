Analyzers
---------

Python-dbms supports **analyzers**. For more information on analyzers, refer
to `DbmsDB manual`_.

.. _DbmsDB manual: https://docs.dbmsdb.com

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Retrieve list of analyzers.
    db.analyzers()

    # Create an analyzer.
    db.create_analyzer(
        name='test_analyzer',
        analyzer_type='identity',
        properties={},
        features=[]
    )

    # Delete an analyzer.
    db.delete_analyzer('test_analyzer', ignore_missing=True)

Refer to :ref:`StandardDatabase` class for API specification.