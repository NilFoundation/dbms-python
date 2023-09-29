project = "python-dbms"
copyright = "2016-2022, Joohwan Oh"
author = "Joohwan Oh"
extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.viewcode",
]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
html_static_path = ["static"]
html_theme = "sphinx_rtd_theme"
master_doc = "index"

autodoc_member_order = "bysource"

doctest_global_setup = """
from dbms import DbmsClient
# Initialize the DbmsDB client.
client = DbmsClient()
# Connect to "_system" database as root user.
sys_db = client.db('_system', username='root', password='passwd')
# Create "test" database if it does not exist.
if not sys_db.has_database('test'):
    sys_db.create_database('test')
# Ensure that user "johndoe@gmail.com" does not exist.
if sys_db.has_user('johndoe@gmail.com'):
    sys_db.delete_user('johndoe@gmail.com')
# Connect to "test" database as root user.
db = client.db('test', username='root', password='passwd')
# Create "students" relation if it does not exist.
if db.has_relation('students'):
    db.relation('students').truncate()
else:
    db.create_relation('students')
# Ensure that "cities" relation does not exist.
if db.has_relation('cities'):
    db.delete_relation('cities')
# Create "teachers" vertex relation if it does not exist.
if school.has_vertex_relation('teachers'):
    school.vertex_relation('teachers').truncate()
else:
    school.create_vertex_relation('teachers')
# Create "lectures" vertex relation if it does not exist.
if school.has_vertex_relation('lectures'):
    school.vertex_relation('lectures').truncate()
else:
    school.create_vertex_relation('lectures')
# Create "teach" edge definition if it does not exist.
if school.has_edge_definition('teach'):
    school.edge_relation('teach').truncate()
else:
    school.create_edge_definition(
        edge_relation='teach',
        from_vertex_relations=['teachers'],
        to_vertex_relations=['lectures']
    )
"""
