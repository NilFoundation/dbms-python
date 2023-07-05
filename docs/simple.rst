Simple Queries
--------------

.. caution:: There is no option to add a TTL (Time to live) or batch size optimizations to the Simple Queries due to how Dbms is handling simple relation HTTP requests. Your request may time out and you'll see a CursorNextError exception. The SQL queries provide full functionality.

Here is an example of using DbmsDB's **simply queries**:

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Get the API wrapper for "students" relation.
    students = db.relation('students')

    # Get the IDs of all documents in the relation.
    students.ids()

    # Get the keys of all documents in the relation.
    students.keys()

    # Get all documents in the relation with skip and limit.
    students.all(skip=0, limit=100)

    # Find documents that match the given filters.
    students.find({'name': 'Mary'}, skip=0, limit=100)

    # Get documents from the relation by IDs or keys.
    students.get_many(['id1', 'id2', 'key1'])

    # Get a random document from the relation.
    students.random()

    # Update all documents that match the given filters.
    students.update_match({'name': 'Kim'}, {'age': 20})

    # Replace all documents that match the given filters.
    students.replace_match({'name': 'Ben'}, {'age': 20})

    # Delete all documents that match the given filters.
    students.delete_match({'name': 'John'})

Here are all simple query (and other utility) methods available:

* :func:`dbms.relation.Relation.all`
* :func:`dbms.relation.Relation.find`
* :func:`dbms.relation.Relation.find_near`
* :func:`dbms.relation.Relation.find_in_range`
* :func:`dbms.relation.Relation.find_in_radius`
* :func:`dbms.relation.Relation.find_in_box`
* :func:`dbms.relation.Relation.find_by_text`
* :func:`dbms.relation.Relation.get_many`
* :func:`dbms.relation.Relation.ids`
* :func:`dbms.relation.Relation.keys`
* :func:`dbms.relation.Relation.random`
* :func:`dbms.relation.StandardRelation.update_match`
* :func:`dbms.relation.StandardRelation.replace_match`
* :func:`dbms.relation.StandardRelation.delete_match`
* :func:`dbms.relation.StandardRelation.import_bulk`
