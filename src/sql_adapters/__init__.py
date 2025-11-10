from typing import NamedTuple
from sqlalchemy import (
    CursorResult as CursorResult,
    delete as delete,
    insert as insert,
    select as select,
    update as update,
    text as text,
    Column as Column,
)
# For setting up tables
from sqlalchemy.orm import (
    declarative_base as declarative_base,
)
from . import sqlite as sqlite
from .utils import param_check as param_check