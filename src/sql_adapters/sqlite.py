"""
SQLite database adapter

This module provides a connector for SQLite databases,
basic context management for sessions, and execution of SQL statements.

It also enables foregin key constraints and uses WAL mode.
"""

from pathlib import Path
from typing import Any, Literal, Mapping, Optional

import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import Connection
from .connector import Connector

from sqlalchemy.dialects.sqlite import *  # noqa: F403

class _Config():
    def __init__(self):
        self.data_dir: Optional[Path | str] = None
        self.default_engine_kwargs = {"pool_size": 100}

Config = _Config()
CONNECTORS = {}

DEFAULT_KWARGS = {"pool_size": 100}

def _resolve_path(title) -> Path:
    global Config
    if Config.data_dir is None:
        raise RuntimeError(
            "Received relative path but DATA_DIR is not set "
            "for sql_adapter.sqlite"
        )
    data_dir = Config.data_dir
    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)

    if not data_dir.exists():
        data_dir.mkdir()

    return data_dir / f"{title}.db"


class SqliteAdapter(Connector):
    """
    A connection to a SQLite database.
    To be inherited by a user adapter class.
    """

    ENGINES = {}

    def __init__(
        self,
        path,
        mode: Literal["ro", "rw"] = "rw",
        timeout=5,
        enable_foreign_keys: bool = True,
        wal_mode: bool = True,
        **engine_kwargs: Optional[Mapping],
    ):
        """
        Initialize the SQLite connector.

        :param path: Path to sqlite database file
        :param mode: Mode for opening the database
        :param timeout: Timeout for database operations
        :param enable_foreign_keys: Enable foreign key constraints
        :param wal_mode: Enable Write-Ahead Logging
        :param engine_kwargs: Additional engine parameters
        """
        if Path(path).is_absolute():
            self.path = Path(path)
        else:
            self.path = _resolve_path(path)

        # hold the open conn if we have one
        self.conn: Optional[Connection] = None

        self.mode = mode
        self.timeout = timeout
        self.enable_foreign_keys = enable_foreign_keys
        self.wal_mode = wal_mode
        if engine_kwargs is None:
            engine_kwargs = {}
        global Config
        _engine_kwargs = Config.default_engine_kwargs | engine_kwargs

        uri = f"{self.path}:{self.mode}"
        if uri in CONNECTORS:
            engine = CONNECTORS[uri]
        else:
            engine = sqlalchemy.create_engine(
                f"sqlite:///{self.connect_string()}",
                **_engine_kwargs,
            )
            CONNECTORS[uri] = engine
        self.engine = engine

    @property
    def connection(self) -> Connection:
        """
        Get the connection to the SQLite database.
        """
        if self.conn is None:
            raise RuntimeError("Database connection not established")
        return self.conn

    def connect_string(self) -> str:
        mode = self.mode
        if mode == "rw":
            mode = "rwc"  # read/write/create

        return f"{self.path}?mode={mode}&timeout={self.timeout}"

    def __enter__(self):
        """Establish a connection to the SQLite database"""
        if self.conn:
            raise RuntimeError(
                "Database connection already established, use __exit__ to close it"
            )

        self.conn = self.engine.connect()
        # conn.__enter__()  # start transaction
        if self.wal_mode:
            # WAL generally more efficient for concurrent reads/writes
            self.conn.execute(text("PRAGMA journal_mode=WAL"))

        if self.enable_foreign_keys:
            # Enable foreign key constraints
            self.conn.execute(text("PRAGMA foreign_keys=ON"))

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None and exc_val is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.__exit__(exc_type, exc_val, exc_tb)
            self.conn.close()
            self.conn = None
