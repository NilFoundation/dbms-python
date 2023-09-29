Documents
---------

In python-dbms, a **document** is a Python dictionary with the following
properties:

* Is JSON serializable.
* May be nested to an arbitrary depth.
* May contain lists.
* Contains the ``_key`` field, which identifies the document uniquely within a
  specific relation.
* Contains the ``_id`` field (also called the *handle*), which identifies the
  document uniquely across all relations within a database. This ID is a
  combination of the relation name and the document key using the format
  ``{relation}/{key}`` (see example below).
* Contains the ``_rev`` field. DbmsDB supports MVCC (Multiple Version
  Concurrency Control) and is capable of storing each document in multiple
  revisions. Latest revision of a document is indicated by this field. The
  field is populated by DbmsDB and is not required as input unless you want
  to validate a document against its current revision.

For more information on documents and associated terminologies, refer to
`DbmsDB manual`_. Here is an example of a valid document in "students"
relation:

.. _DbmsDB manual: https://docs.dbmsdb.com

.. testcode::

    {
        '_id': 'students/bruce',
        '_key': 'bruce',
        '_rev': '_Wm3dzEi--_',
        'first_name': 'Bruce',
        'last_name': 'Wayne',
        'address': {
            'street' : '1007 Mountain Dr.',
            'city': 'Gotham',
            'state': 'NJ'
        },
        'is_rich': True,
        'friends': ['robin', 'gordon']
    }
