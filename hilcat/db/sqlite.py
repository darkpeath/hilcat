import os
import re
import abc
import warnings
from typing import (
    List, Sequence, Any,
    Hashable, Dict, Union,
    Literal,
)
from .relational import (
    BaseRelationalDbCache,
    RelationalDbScopeConfig,
    RelationalDbCache,
    SingleTableCache,
    Operation,
    QmarkSqlBuilder, ValueAdapter,
)
import sqlite3

class SqliteSqlBuilder(QmarkSqlBuilder):
    default_column_type = "str"

    def build_select_all_table_operation(self) -> Operation:
        return Operation(statement="SELECT name FROM sqlite_master WHERE type = 'table'")

    def build_select_table_columns_operation(self, table: str, filter_uniq=False) -> Operation:
        return Operation(statement=f"PRAGMA table_info({table})")

    def get_column_name_from_result(self, result: Sequence[Any]) -> str:
        # second is column name
        return result[1]

class SqliteScopeConfig(RelationalDbScopeConfig):
    def __init__(self, scope: Hashable, table: str = None, uniq_column: str = None,
                 uniq_columns: Sequence[str] = ('id',), columns: Sequence[str] = ('data',),
                 column_types: Dict[str, str] = None,
                 value_adapter: Union[Literal['auto', 'default', 'single', 'tuple', 'list'], ValueAdapter] = 'auto',
                 default_column_type="str"):
        warnings.warn("use RelationalDbScopeConfig instead", DeprecationWarning)
        super().__init__(scope, table, uniq_column, uniq_columns, columns, column_types, value_adapter,
                         default_column_type)

class BaseSqliteCache(BaseRelationalDbCache, abc.ABC):
    """
    Base abstract class for sqlite backend.
    """

    api_module = sqlite3
    paramstyle = 'qmark'
    sql_builder = SqliteSqlBuilder()

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'BaseSqliteCache':
        assert re.match(r'\w+:///.+', uri), uri
        schema, database = uri.split(':///')
        return cls(database=database, **kwargs)

    def connect_db(self, database: str = None, connect_args: Dict[str, Any] = None):
        # ensure database directory exists
        os.makedirs(os.path.dirname(database), exist_ok=True)
        return super().connect_db(database, connect_args)

    def _get_unique_columns(self, table: str) -> List[Sequence[Any]]:
        columns = self._get_table_columns(table)
        return [x for x in columns if x[5] > 0]

    def _execute_many0(self, cursor, operation: Operation):
        # if executemany, no parameters should be given
        if operation.parameters:
            raise ValueError("there should be no parameters when invoke executemany()")
        # if semicolon in a stmt, it should be quoted
        stmts = operation.statement.split(";")
        for stmt in stmts:
            cursor.execute(stmt.strip())

class SqliteCache(BaseSqliteCache, RelationalDbCache):
    """
    Use a sqlite database as backend.
    """

class SqliteSingleTableCache(BaseSqliteCache, SingleTableCache):
    """
    Use one table of sqlite as backend.
    """
