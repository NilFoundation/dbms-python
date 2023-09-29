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

Please see the [documentation](https://docs.python-dbms.com) for more details.
