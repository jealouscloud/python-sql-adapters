# sql-adapters

A Python library that wraps SQLAlchemy core to deliver an opinionated style
of development wherein databases are implemented as adapters which are interfaces
to the data source.


```python
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


class DemoAdapter(sql.SqliteAdapter):
    def __init__(
        self,
        read_only: bool = False,
    ):
        super().__init__("test", mode="ro" if read_only else "rw")

    @staticmethod
    def init():
        adapter = DemoAdapter(read_only=False)
        with adapter:
            base.metadata.create_all(adapter.connection)

    def add_item(self, name: str):
        self.execute(insert(_TestTable).values(name=name))

    def get_item(self, item_id: int) -> returns.select_result:
        stmt = select(_TestTable).where(_TestTable.id == item_id)
        result = self.execute(stmt)
        data = self.read_values(result, returns.select_result)
        return next(data)
```