Schema Validation
-----------------

DbmsDB supports document validation using JSON schemas. You can use this
feature by providing a schema during relation creation using the ``schema``
parameter.

**Example:**

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    if db.has_relation('employees'):
        db.delete_relation('employees')

    # Create a new relation named "employees" with custom schema.
    my_schema = {
        'rule': {
            'type': 'object',
            'properties': {
                'name': {'type': 'string'},
                'email': {'type': 'string'}
            },
            'required': ['name', 'email']
        },
        'level': 'moderate',
        'message': 'Schema Validation Failed.'
    }
    employees = db.create_relation(name='employees', schema=my_schema)

    # Modify the schema.
    employees.configure(schema=my_schema)

    # Remove the schema.
    employees.configure(schema={})
