# -*- coding: utf-8 -*-

from typing import List
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

class SqliteScopeConfig(RelationalDbScopeConfig):
    def get_column_type(self, col: str) -> str:
        return self.column_types.get(col, 'str')

class SqliteCache(RelationalDbCache):
    """
    Use a sqlite database as backend.
    """

    api_module = sqlite3
    paramstyle = 'qmark'
    sql_builder = SqliteSqlBuilder()

    def _get_table_column_names(self, table: str) -> List[str]:
        columns = self._get_table_columns(table)
        # second is column name
        return [x[1] for x in columns]

    def _get_unique_column_name(self, table: str) -> str:
        columns = self._get_table_columns(table)
        columns = [x for x in columns if x[5] > 0]
        if len(columns) != 1:
            raise ValueError(f"There should be exactly one uniq column, but {len(columns)} has given.")
        return columns[1][0]

    def _execute_many0(self, cursor, operation: Operation):
        # if executemany, no parameters should be given
        if operation.parameters:
            raise ValueError("there should be no parameters when invoke executemany()")
        # if semicolon in a stmt, it should be quoted
        stmts = operation.statement.split(";")
        for stmt in stmts:
            cursor.execute(stmt.strip())
