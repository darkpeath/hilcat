# -*- coding: utf-8 -*-

from typing import (
    Dict, Any, Tuple, Sequence,
    Hashable, Union, Literal,
)
import warnings
from abc import ABC, abstractmethod
from .relational import (
    FormatSqlBuilder,
    BaseRelationalDbCache,
    RelationalDbScopeConfig,
    RelationalDbCache,
    SingleTableCache,
    Operation, ValueAdapter,
)

class MysqlSqlBuilder(FormatSqlBuilder):

    default_column_type = 'text'

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

    def get_column_name_from_result(self, result: Sequence[Any]) -> str:
        # 4th is column name.
        return result[4]

    def _gen_update_statement(self, config: 'MysqlScopeConfig', value: Dict[str, Any]) -> Tuple[str, bool]:
        first = ','.join(self.config_variable(name=k, order=i, value=v)
                         for i, (k, v) in enumerate(value.items(), 1))
        second = ','.join(f'{k}={self.config_variable(name=k, order=i, value=v)}'
                          for i, (k, v) in enumerate(value.items(), 1))
        return (f"INSERT INTO {config.table}({','.join(value.keys())})"
                f" VALUES ({first})"
                f" ON DUPLICATE KEY"
                f" UPDATE {second}"), False

class MysqlScopeConfig(RelationalDbScopeConfig):
    def __init__(self, scope: Hashable, table: str = None, uniq_column: str = None,
                 uniq_columns: Sequence[str] = ('id',), columns: Sequence[str] = ('data',),
                 column_types: Dict[str, str] = None,
                 value_adapter: Union[Literal['auto', 'default', 'single', 'tuple', 'list'], ValueAdapter] = 'auto',
                 default_column_type: str = None):
        warnings.warn("use RelationalDbScopeConfig instead", DeprecationWarning)
        super().__init__(scope, table, uniq_column, uniq_columns, columns, column_types, value_adapter,
                         default_column_type)

class BaseBackend(BaseRelationalDbCache, ABC):
    """
    Base abstract class for mysql backend.
    """
    paramstyle = 'format'
    sql_builder = MysqlSqlBuilder()

    @abstractmethod
    def _connect_db0(self, **kwargs):
        """
        The actual method to connect database .
        """
        pass

    def connect_db(self, database: str = None, connect_args: Dict[str, Any] = None):
        kwargs = dict(connect_args or {})
        if database is not None:
            # TODO 2023/10/12  parse uri
            import warnings
            warnings.warn("uri is ignored")
            return self._connect_db0(**kwargs)

class PymysqlBackend(BaseBackend, ABC):
    def _connect_db0(self, **kwargs):
        import pymysql
        return pymysql.connect(**kwargs)

class MysqlConnectorBackend(BaseBackend, ABC):
    def _connect_db0(self, **kwargs):
        import mysql.connector
        return mysql.connector.connect(**kwargs)

try:
    import pymysql
    BaseMysqlCache = PymysqlBackend
except ImportError:
    import mysql.connector
    BaseMysqlCache = MysqlConnectorBackend

class MysqlCache(BaseMysqlCache, RelationalDbCache):
    """
    Use a mysql database as backend.
    """

class MysqlSingleTableCache(BaseMysqlCache, SingleTableCache):
    """
    Use one table of mysql as backend.
    """
