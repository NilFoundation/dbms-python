![Python version](https://img.shields.io/badge/python-3.7%2B-blue)

# =nil; `DROP DATABASE * Python SDK

Python driver for =nil; `DROP DATABASE *.

## Requirements

- Python version 3.7+

## Installation

```shell
pip3 install wheel
pip3 install .
```

## Getting Started

Here is a simple usage example:

```python
from dbms import DbmsClient

# Initialize the client for DbmsDB.
client = DbmsClient(hosts="http://localhost:8529")

# Connect to "_system" database as root user.
sys_db = client.db("_system", username="root", password="")

# Create a new database named "test".
sys_db.create_database("test")

# Connect to "test" database as root user.
db = client.db("test", username="root", password="")

# Create a new relation named "students".
students = db.create_relation("students")

# Add a hash index to the relation.
students.add_hash_index(fields=["name"], unique=True)

# Insert new documents into the relation.
students.insert({"name": "jane", "age": 39})
students.insert({"name": "josh", "age": 18})
students.insert({"name": "judy", "age": 21})

# Execute an SQL query and iterate through the result cursor.
cursor = db.sql.execute("FOR doc IN students RETURN doc")
student_names = [document["name"] for document in cursor]
```

Another example with [graphs](https://www.dbmsdb.com/docs/stable/graphs.html):

```python
from dbms import DbmsClient

# Initialize the client for DbmsDB.
client = DbmsClient(hosts="http://localhost:8529")

# Connect to "test" database as root user.
db = client.db("test", username="root", password="")

# Create a new graph named "school".
graph = db.create_graph("school")

# Create vertex relations for the graph.
students = graph.create_vertex_relation("students")
lectures = graph.create_vertex_relation("lectures")

# Create an edge definition (relation) for the graph.
edges = graph.create_edge_definition(
    edge_relation="register",
    from_vertex_relations=["students"],
    to_vertex_relations=["lectures"]
)

# Insert vertex documents into "students" (from) vertex relation.
students.insert({"_key": "01", "full_name": "Anna Smith"})
students.insert({"_key": "02", "full_name": "Jake Clark"})
students.insert({"_key": "03", "full_name": "Lisa Jones"})

# Insert vertex documents into "lectures" (to) vertex relation.
lectures.insert({"_key": "MAT101", "title": "Calculus"})
lectures.insert({"_key": "STA101", "title": "Statistics"})
lectures.insert({"_key": "CSC101", "title": "Algorithms"})

# Insert edge documents into "register" edge relation.
edges.insert({"_from": "students/01", "_to": "lectures/MAT101"})
edges.insert({"_from": "students/01", "_to": "lectures/STA101"})
edges.insert({"_from": "students/01", "_to": "lectures/CSC101"})
edges.insert({"_from": "students/02", "_to": "lectures/MAT101"})
edges.insert({"_from": "students/02", "_to": "lectures/STA101"})
edges.insert({"_from": "students/03", "_to": "lectures/CSC101"})

# Traverse the graph in outbound direction, breadth-first.
result = graph.traverse(
    start_vertex="students/01",
    direction="outbound",
    strategy="breadthfirst"
)
```

Please see the [documentation](https://docs.python-dbms.com) for more details.
