# -*- coding: utf-8 -*-

"""
Sqlite can be used as a cache or a persistence storage.
Actually, implement a cache is enough.
"""

from typing import (
    Any, Dict, List,
)
import sqlite3
from .relational import (
    RelationalDbScopeConfig,
    RelationalDbCache, Operation
)

class SqliteScopeConfig(RelationalDbScopeConfig):
    def get_column_type(self, col: str) -> str:
        return self.column_types.get(col, 'str')

class SqliteCache(RelationalDbCache):
    """
    Use a sqlite database as backend.
    """

    api_module = sqlite3

    def _get_all_tables_in_db(self) -> List[str]:
        # first is table name
        return [x[0] for x in self._execute("SELECT name FROM sqlite_master WHERE type = 'table'", fetch_size='all')]

    def _get_table_columns(self, table: str) -> List[Dict[str, Any]]:
        columns = list(self._execute(f"PRAGMA table_info({table})", fetch_size='all'))
        assert len(columns[0]) == 6
        return [{
            "name": x[1],       # second is column name
            "is_primary_key": x[5] > 0,
        } for x in columns]

    def _execute_many0(self, cursor, operation: Operation):
        # if executemany, no parameters should be given
        if operation.parameters:
            raise ValueError("there should be no parameters when invoke executemany()")
        stmts = operation.template.split(";")
        for stmt in stmts:
            cursor.execute(stmt)
