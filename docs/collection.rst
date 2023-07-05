Relations
-----------

A **relation** contains :doc:`documents <document>`. It is uniquely identified
by its name which must consist only of hyphen, underscore and alphanumeric
characters. There are three types of relations in python-dbms:

* **Standard Relation:** contains regular documents.
* **Vertex Relation:** contains vertex documents for graphs. See
  :ref:`here <vertex-relations>` for more details.
* **Edge Relation:** contains edge documents for graphs. See
  :ref:`here <edge-relations>` for more details.

Here is an example showing how you can manage standard relations:

.. testcode::

    from dbms import DbmsClient

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # List all relations in the database.
    db.relations()

    # Create a new relation named "students" if it does not exist.
    # This returns an API wrapper for "students" relation.
    if db.has_relation('students'):
        students = db.relation('students')
    else:
        students = db.create_relation('students')

    # Retrieve relation properties.
    students.name
    students.db_name
    students.properties()
    students.revision()
    students.statistics()
    students.checksum()
    students.count()

    # Perform various operations.
    students.load()
    students.unload()
    students.truncate()
    students.configure()

    # Delete the relation.
    db.delete_relation('students')

See :ref:`StandardDatabase` and :ref:`StandardRelation` for API specification.
