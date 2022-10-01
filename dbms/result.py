__all__ = ["Result"]

from typing import TypeVar, Union

from dbms.job import AsyncJob, BatchJob

T = TypeVar("T")

Result = Union[T, AsyncJob[T], BatchJob[T], None]
