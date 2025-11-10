from abc import ABC, abstractmethod
from typing import Any, Iterator, NamedTuple, Optional, TypeVar

from sqlalchemy import CursorResult, Executable
from sqlalchemy.engine import Connection
from sqlalchemy.engine.interfaces import (
    CoreExecuteOptionsParameter,
    _CoreAnyExecuteParams,
)

from .utils import param_check

T = TypeVar("T", bound=NamedTuple)


class Connector(ABC):
    """
    An abstract base class for SQL database connectors.
    """

    @property
    @abstractmethod
    def connection(self) -> Connection:
        """
        Get the connection to database.
        """
        pass

    def execute(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
        *args,
        **kwargs,
    ) -> CursorResult[Any]:
        """
        Execute a SQL statement.  

        :param statement: The SQL statement to execute.  
        :param parameters: Optional parameters for the SQL statement.  
        :param execution_options: Optional execution options for the SQL statement.  
        :param args: Additional positional arguments passed to connection.execute().
        :param kwargs: Additional keyword arguments passed to connection.execute().

        :return: The result of the SQL statement execution.  
        """  # fmt: skip
        result = self.connection.execute(
            statement,
            parameters=parameters,
            execution_options=execution_options,
            *args,
            **kwargs,
        )
        return result

    def read_values(
        self, result: CursorResult, result_type: type[T]
    ) -> Iterator[T]:
        """
        Translate result rows into NamedTuple of given type.

        :param result: Cursor result from running execute on a SQL query
        :param result_type: The type of NamedTuple to marshal rows into
        :return: Iterator of result_type instances
        """
        param_check(result, result_type)

        for row in result:
            yield result_type(*row)  # type: ignore[arg-type]

    def result_of(
        self,
        result_type: type[T],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
        *args,
        **kwargs,
    ) -> Iterator[T]:
        """
        Execute a statement and yield results as NamedTuple of given type.

        :param result_type: The NamedTuple type to marshal results to
        :param statement: The SQL statement to execute
        :param parameters: Optional parameters for the SQL statement.
        :param execution_options: Optional execution options for the SQL statement.
        :param args: Additional positional arguments passed to connection.execute().
        :param kwargs: Additional keyword arguments passed to connection.execute().
        :return: Result values in an iterator
        :rtype: Iterator[T]
        """
        result = self.execute(
            statement,
            parameters=parameters,
            execution_options=execution_options,
            *args,
            **kwargs,
        )
        return self.read_values(result, result_type)

    @abstractmethod
    def __enter__(self):
        """Establish a connection to the database and start transaction"""
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the connection to the database,
        committing or rolling back transaction
        """
        pass
