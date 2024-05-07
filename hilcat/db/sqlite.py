# -*- coding: utf-8 -*-

import re
import warnings
import dataclasses
from typing import List, Sequence, Any
from .relational import (
    RelationalDbScopeConfig,
    RelationalDbCache,
    Operation,
    QmarkSqlBuilder,
)
import sqlite3

class SqliteSqlBuilder(QmarkSqlBuilder):
    def build_select_all_table_operation(self) -> Operation:
        return Operation(statement="SELECT name FROM sqlite_master WHERE type = 'table'")

    def build_select_table_columns_operation(self, table: str, filter_uniq=False) -> Operation:
        return Operation(statement=f"PRAGMA table_info({table})")

    def get_column_name_from_result(self, result: Sequence[Any]) -> str:
        # second is column name
        return result[1]

@dataclasses.dataclass
class SqliteScopeConfig(RelationalDbScopeConfig):
    default_column_type = "str"
    def __post_init__(self):
        warnings.warn("use RelationalDbScopeConfig instead", DeprecationWarning)
        super().__post_init__()

class SqliteCache(RelationalDbCache):
    """
    Use a sqlite database as backend.
    """

    api_module = sqlite3
    paramstyle = 'qmark'
    sql_builder = SqliteSqlBuilder()

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'RelationalDbCache':
        assert re.match(r'\w+:///.+', uri), uri
        schema, database = uri.split(':///')
        return cls(database=database, **kwargs)

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
