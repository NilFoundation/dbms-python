Indexes
-------

**Indexes** can be added to relations to speed up document lookups. Every
relation has a primary hash index on ``_key`` field by default. This index
cannot be deleted or modified. Every edge relation has additional indexes
on fields ``_from`` and ``_to``. For more information on indexes, refer to
`DbmsDB manual`_.

.. _DbmsDB manual: https://docs.dbmsdb.com

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Create a new relation named "cities".
    cities = db.create_relation('cities')

    # List the indexes in the relation.
    cities.indexes()

    # Add a new hash index on document fields "continent" and "country".
    index = cities.add_hash_index(fields=['continent', 'country'], unique=True)

    # Add a new skiplist index on field 'population'.
    index = cities.add_skiplist_index(fields=['population'], sparse=False)

    # Add a new persistent index on field 'currency'.
    index = cities.add_persistent_index(fields=['currency'], sparse=True)

    # Add a new TTL (time-to-live) index on field 'currency'.
    index = cities.add_ttl_index(fields=['ttl'], expiry_time=200)

    # Indexes may be added with a name that can be referred to in SQL queries.
    index = cities.add_hash_index(fields=['country'], name='my_hash_index')

    # Delete the last index from the relation.
    cities.delete_index(index['id'])

See :ref:`StandardRelation` for API specification.
