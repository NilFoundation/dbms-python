Error Codes
-----------

Python-dbms provides DbmsDB error code constants for convenience.

**Example**

.. testcode::

    from dbms import errno

    # Some examples
    assert errno.NOT_IMPLEMENTED == 9
    assert errno.DOCUMENT_REV_BAD == 1239
    assert errno.DOCUMENT_NOT_FOUND == 1202

For more information, refer to `DbmsDB manual`_.

.. _DbmsDB manual: https://www.dbmsdb.com/docs/stable/appendix-error-codes.html
