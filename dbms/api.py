__all__ = ["ApiGroup"]

from typing import Callable, Optional, TypeVar

from dbms.connection import Connection
from dbms.executor import ApiExecutor
from dbms.request import Request
from dbms.response import Response
from dbms.result import Result

T = TypeVar("T")


class ApiGroup:
    """Base class for API groups.

    :param connection: HTTP connection.
    :param executor: API executor.
    """

    def __init__(self, connection: Connection, executor: ApiExecutor) -> None:
        self._conn = connection
        self._executor = executor

    @property
    def conn(self) -> Connection:
        """Return the HTTP connection.

        :return: HTTP connection.
        :rtype: dbms.connection.BasicConnection | dbms.connection.JwtConnection |
            dbms.connection.JwtSuperuserConnection
        """
        return self._conn

    @property
    def db_name(self) -> str:
        """Return the name of the current database.

        :return: Database name.
        :rtype: str
        """
        return self._conn.db_name

    @property
    def username(self) -> Optional[str]:
        """Return the username.

        :returns: Username.
        :rtype: str
        """
        return self._conn.username

    @property
    def context(self) -> str:
        """Return the API execution context.

        :return: API execution context. Possible values are "default", "async",
            "batch" and "transaction".
        :rtype: str
        """
        return self._executor.context

    def _execute(
        self, request: Request, response_handler: Callable[[Response], T]
    ) -> Result[T]:
        """Execute an API.

        :param request: HTTP request.
        :type request: dbms.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result.
        """
        return self._executor.execute(request, response_handler)
