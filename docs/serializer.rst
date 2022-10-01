JSON Serialization
------------------

You can provide your own JSON serializer and deserializer during client
initialization. They must be callables that take a single argument.

**Example:**

.. testcode::

    import json

    from dbms import DbmsClient

    # Initialize the DbmsDB client with custom serializer and deserializer.
    client = DbmsClient(
        hosts='http://localhost:8529',
        serializer=json.dumps,
        deserializer=json.loads
    )

See :ref:`DbmsClient` for API specification.