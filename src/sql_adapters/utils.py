from typing import NamedTuple
from . import sqlite as sqlite

from sqlalchemy import (
    CursorResult as CursorResult,
    delete as delete,
    insert as insert,
    select as select,
    update as update,
)


def param_check(result: CursorResult, named_tuple: type[NamedTuple]):
    """
    This is a deceivingly simple check to ensure that the database query
    result matches the data type we expect to marshal it to.

    When querying, you can use .label() to rename columns in the query
    to match the NamedTuple fields.

    :param result: The SQLAlchemy CursorResult object from the query.
    :param named_tuple: The NamedTuple type to check against.
    :raises ValueError: If the result columns do not match the NamedTuple fields.
    """
    keys = tuple(result.keys())
    if keys != named_tuple._fields:
        raise ValueError(
            f"SQL returned keys {keys}, but type expected {named_tuple._fields}. "
            "Do you need .label() in your select query?"
        )
