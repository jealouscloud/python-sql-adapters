from datetime import datetime
from turtle import st
from typing import Literal, NamedTuple

import sql_adapters.sqlite as sql
from sql_adapters import (
    Column,
    declarative_base,
    delete,
    insert,
    select,
    text,
    update,
)

base = declarative_base()


class _TestTable(base):
    __tablename__ = "test_table"

    id = Column(sql.INTEGER, primary_key=True, autoincrement=True)
    name = Column(sql.TEXT, nullable=False)
    date_created = Column(
        sql.TZDateTime,
        nullable=False,
        default=lambda: datetime.now().astimezone(),
    )


class returns:
    class select_result(NamedTuple):
        id: int
        name: str
        date_created: datetime


class _TestAdapter(sql.SqliteAdapter):
    def __init__(
        self,
        read_only: bool = False,
    ):
        super().__init__("test", mode="ro" if read_only else "rw")

    @staticmethod
    def init():
        adapter = _TestAdapter(read_only=False)
        with adapter:
            base.metadata.create_all(adapter.connection)

    def add_item(self, name: str):
        self.execute(insert(_TestTable).values(name=name))

    def get_item(self, item_id: int) -> returns.select_result:
        stmt = select(_TestTable).where(_TestTable.id == item_id)
        result = self.execute(stmt)
        data = self.read_values(result, returns.select_result)
        return next(data)


class select_result(NamedTuple):
    id: int
    name: str


def test_sqlite_adapter(tmp_path):
    sql.Config.data_dir = tmp_path

    _TestAdapter.init()
    adapter = _TestAdapter(read_only=False)

    with adapter:
        adapter.execute(insert(_TestTable).values(name="test_name_1"))
        now = datetime.now().astimezone()
        result = adapter.execute(select(_TestTable.id, _TestTable.name))
        data = adapter.read_values(result, select_result)
        one = next(data, None)
        assert one == select_result(id=1, name="test_name_1")

        ts = select(_TestTable.date_created).where(_TestTable.id == 1)
        result = adapter.execute(ts)
        r = result.one()
        assert isinstance(r[0], datetime)

        update_stmt = (
            update(_TestTable)
            .where(_TestTable.id == 1)
            .values(date_created=now)
        )
        result = adapter.execute(update_stmt)
        ts = select(_TestTable.date_created).where(_TestTable.id == 1)
        result = adapter.execute(ts)
        r = result.one()
        assert r[0] == now
