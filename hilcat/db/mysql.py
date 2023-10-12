# -*- coding: utf-8 -*-

from typing import (
    Dict, Any, Tuple,
)
from .relational import (
    FormatSqlBuilder,
    RelationalDbScopeConfig,
    RelationalDbCache, Operation,
)
try:
    import mysql.connector as mysql_connector
except ImportError:
    import pymysql as mysql_connector

class MysqlSqlBuilder(FormatSqlBuilder):

    def get_value_type(self, value: Any) -> str:
        return 's'

    def build_select_all_table_operation(self) -> Operation:
        return Operation(statement="SHOW TABLES")

    def build_select_table_columns_operation(self, table: str, filter_uniq=False) -> Operation:
        if filter_uniq:
            stmt = f"SHOW INDEX FROM {table} WHERE key_name = 'PRIMARY'"
        else:
            stmt = f"DESCRIBE {table}"
        return Operation(statement=stmt)

    def _gen_update_statement(self, config, value: Dict[str, Any]) -> Tuple[str, bool]:
        first = ','.join(self.config_variable(name=k, order=i, value=v)
                         for i, (k, v) in enumerate(value.items(), 1))
        second = ','.join(f'{k}={self.config_variable(name=k, order=i, value=v)}'
                          for i, (k, v) in enumerate(value.items(), 1))
        return (f"INSERT INTO {config.scope}({','.join(value.keys())})"
                f" VALUES ({first})"
                f" ON DUPLICATE KEY"
                f" UPDATE {second}"), False
class MysqlScopeConfig(RelationalDbScopeConfig):
    def get_column_type(self, col: str) -> str:
        return self.column_types.get(col, 'text')

class MysqlCache(RelationalDbCache):
    """
    Use mysql database as backend.
    """

    api_module = mysql_connector
    sql_builder = MysqlSqlBuilder()

    def connect_db(self, database: str = None, connect_args: Dict[str, Any] = None):
        kwargs = dict(connect_args or {})
        if database is not None:
            # TODO 2023/10/12  parse uri
            import warnings
            warnings.warn("uri is ignored")
        return self.api_module.connect(**kwargs)

    def _get_unique_column_name(self, table: str) -> str:
        operation = self.sql_builder.build_select_table_columns_operation(table, filter_uniq=True)
        columns = self._execute(operation, fetch_size='all')
        if len(columns) != 1:
            raise ValueError(f"There should be exactly one uniq column, but {len(columns)} has given.")
        # 4th is column name.
        return columns[0][4]

