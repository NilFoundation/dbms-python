Error Handling
--------------

All python-dbms exceptions inherit :class:`dbms.exceptions.DbmsError`,
which splits into subclasses :class:`dbms.exceptions.DbmsServerError` and
:class:`dbms.exceptions.DbmsClientError`.

Server Errors
=============

:class:`dbms.exceptions.DbmsServerError` exceptions lightly wrap non-2xx
HTTP responses coming from DbmsDB. Each exception object contains the error
message, error code and HTTP request response details.

**Example:**

.. testcode::

    from dbms import DbmsClient, DbmsServerError, DocumentInsertError

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Get the API wrapper for "students" relation.
    students = db.relation('students')

    try:
        students.insert({'_key': 'John'})
        students.insert({'_key': 'John'})  # duplicate key error

    except DocumentInsertError as exc:

        assert isinstance(exc, DbmsServerError)
        assert exc.source == 'server'

        exc.message           # Exception message usually from DbmsDB
        exc.error_message     # Raw error message from DbmsDB
        exc.error_code        # Error code from DbmsDB
        exc.url               # URL (API endpoint)
        exc.http_method       # HTTP method (e.g. "POST")
        exc.http_headers      # Response headers
        exc.http_code         # Status code (e.g. 200)

        # You can inspect the DbmsDB response directly.
        response = exc.response
        response.method       # HTTP method (e.g. "POST")
        response.headers      # Response headers
        response.url          # Full request URL
        response.is_success   # Set to True if HTTP code is 2XX
        response.body         # JSON-deserialized response body
        response.raw_body     # Raw string response body
        response.status_text  # Status text (e.g "OK")
        response.status_code  # Status code (e.g. 200)
        response.error_code   # Error code from DbmsDB

        # You can also inspect the request sent to DbmsDB.
        request = exc.request
        request.method        # HTTP method (e.g. "post")
        request.endpoint      # API endpoint starting with "/_api"
        request.headers       # Request headers
        request.params        # URL parameters
        request.data          # Request payload

See :ref:`Response` and :ref:`Request` for reference.

Client Errors
=============

:class:`dbms.exceptions.DbmsClientError` exceptions originate from
python-dbms client itself. They do not contain error codes nor HTTP request
response details.

**Example:**

.. testcode::

    from dbms import DbmsClient, DbmsClientError, DocumentParseError

    # Initialize the DbmsDB client.
    client = DbmsClient()

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Get the API wrapper for "students" relation.
    students = db.relation('students')

    try:
        students.get({'_id': 'invalid_id'})  # malformed document

    except DocumentParseError as exc:

        assert isinstance(exc, DbmsClientError)
        assert exc.source == 'client'

        # Only the error message is set.
        error_message = exc.message
        assert exc.error_code is None
        assert exc.error_message is None
        assert exc.url is None
        assert exc.http_method is None
        assert exc.http_code is None
        assert exc.http_headers is None
        assert exc.response is None
        assert exc.request is None

Exceptions
==========

Below are all exceptions from python-dbms.

.. automodule:: dbms.exceptions
    :members:


Error Codes
===========

The `errno` module contains a constant mapping to `DbmsDB's error codes
<https://www.dbmsdb.com/docs/stable/appendix-error-codes.html>`_.

.. automodule:: dbms.errno
    :members:
