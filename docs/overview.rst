Getting Started
---------------

Here is an example showing how **python-dbms** client can be used:

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient(hosts='http://localhost:8529')

    # Connect to "_system" database as root user.
    # This returns an API wrapper for "_system" database.
    sys_db = client.db('_system', username='root', password='passwd')

    # Create a new database named "test" if it does not exist.
    if not sys_db.has_database('test'):
        sys_db.create_database('test')

    # Connect to "test" database as root user.
    # This returns an API wrapper for "test" database.
    db = client.db('test', username='root', password='passwd')

    # Create a new relation named "students" if it does not exist.
    # This returns an API wrapper for "students" relation.
    if db.has_relation('students'):
        students = db.relation('students')
    else:
        students = db.create_relation('students')

    # Add a hash index to the relation.
    students.add_hash_index(fields=['name'], unique=False)

    # Truncate the relation.
    students.truncate()

    # Insert new documents into the relation.
    students.insert({'name': 'jane', 'age': 19})
    students.insert({'name': 'josh', 'age': 18})
    students.insert({'name': 'jake', 'age': 21})

    # Execute an SQL query. This returns a result cursor.
    cursor = db.sql.execute('FOR doc IN students RETURN doc')

    # Iterate through the cursor to retrieve the documents.
    student_names = [document['name'] for document in cursor]
