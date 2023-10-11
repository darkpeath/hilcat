# -*- coding: utf-8 -*-

from typing import Any
from .relational import (
    Operation,
    FormatSqlBuilder,
    RelationalDbCache,
    RelationalDbScopeConfig,
)
import psycopg

class PostgresqlBuilder(FormatSqlBuilder):

    def get_value_type(self, value: Any) -> str:
        return 's'

    def build_select_all_table_operation(self) -> Operation:
        return Operation(statement="SELECT tablename FROM pg_tables WHERE schemaname = 'public'")

    def build_select_table_columns_operation(self, table: str, filter_uniq=False) -> Operation:
        if filter_uniq:
            stmt = (f"SELECT a.attname FROM pg_index i"
                    f" JOIN pg_attribute a"
                    f" ON a.attrelid = i.indrelid AND a.attnum = any(i.indkey)"
                    f" WHERE i.indrelid = '{table}'::regclass AND i.indisprimary")
        else:
            stmt = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
        return Operation(statement=stmt)

class PostgresqlScopeConfig(RelationalDbScopeConfig):
    def get_column_type(self, col: str) -> str:
        return self.column_types.get(col, 'text')

class PostgresqlCache(RelationalDbCache):

    api_module = psycopg
    paramstyle = "format"
    sql_builder = PostgresqlBuilder()

    def _execute_many0(self, cursor, operation: Operation):
        # Nothing happened when use cursor.executemany() to create multi tables.
        # So it's needed to split statement.

        # if executemany, no parameters should be given
        if operation.parameters:
            raise ValueError("there should be no parameters when invoke executemany()")
        # if semicolon in a stmt, it should be quoted
        stmts = operation.statement.split(";")
        for stmt in stmts:
            cursor.execute(stmt.strip())

