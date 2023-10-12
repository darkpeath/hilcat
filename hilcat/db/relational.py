# -*- coding: utf-8 -*-

"""
Relational database can be used as a cache or a persistence storage.
Actually, implement a cache is enough.
"""

from typing import (
    Any, Iterable, Dict,
    Optional, List,
    Sequence, Callable,
    Literal, Union,
    Tuple,
)
from abc import ABC, abstractmethod
from types import ModuleType
import dataclasses
from ..core import Cache

_FETCH_SIZE_TYPE = Union[Literal['one', 'all'], int]
_EXECUTE_PARAM_TYPE = Union[Sequence[Any], Dict[str, Any]]
_KEY_TYPE = Union[str, int]

@dataclasses.dataclass
class RelationalDbScopeConfig:
    scope: str
    table: str = None
    uniq_column: str = 'id'     # unique column to identify rows
    columns: Sequence[str] = ('id', 'data')
    columns_with_id: List[str] = dataclasses.field(init=False)

    # if column not specified here, type should be str
    column_types: Dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if self.uniq_column in self.columns:
            self.columns_with_id = list(self.columns)
        else:
            self.columns_with_id = [self.uniq_column] + list(self.columns)
        if not self.table:
            self.table = self.scope

    def get_column_type(self, col: str) -> str:
        # this method should be overwritten for certain database
        return self.column_types.get(col, 'text')

@dataclasses.dataclass
class Operation:
    """
    define an operation for execute() or executemany()
    """
    statement: str       # arg operation for cursor.execute()
    parameters: _EXECUTE_PARAM_TYPE = dataclasses.field(default_factory=list)  # arg parameters for cursor.execute()

    # many statements in template or not, if True, use cursor.executemany() instead of cursor.execute()
    many: bool = False

class SqlBuilder(ABC):
    """
    Build select, update, delete sql for relational database.
    It's used by relational cache to manipulate data.
    """

    def build_select_all_table_operation(self) -> Operation:
        """
        Generate sql to select all tables in the database.
        Table name as first selected column.
        """
        raise NotImplementedError()

    def build_select_table_columns_operation(self, table: str, filter_uniq=False) -> Operation:
        """
        Generate sql to select all columns for given table.
        Column name as first selected result.
        :param table:           the queried table
        :param filter_uniq:     return only id column
        """
        raise NotImplementedError()

    @abstractmethod
    def build_create_table_operation(self, *configs: RelationalDbScopeConfig, check_exists=True):
        """
        Generate sql to create a table.
        """

    @abstractmethod
    def build_select_operation(self, config: RelationalDbScopeConfig, key: _KEY_TYPE = None, limit: int = -1,
                               select_columns: Sequence[str] = None) -> Operation:
        """
        Generate sql to select row for given key.
        :param config:          scope configuration
        :param key:             uniq key for the row, maybe `None` to select all rows in the table
        :param limit:           limit number
        :param select_columns:  columns to be selected, if not set, select all in config
        """

    @abstractmethod
    def build_update_operation(self, config: RelationalDbScopeConfig,
                               key: _KEY_TYPE, value: Dict[str, Any]) -> Operation:
        """
        Generate sql to update or insert row for given key with given value.
        :param config:      scope configuration
        :param key:         uniq key for the row
        :param value:       column values for given uniq key
        """

    @abstractmethod
    def build_delete_operation(self, config: RelationalDbScopeConfig, key: _KEY_TYPE = None) -> Operation:
        """
        Generate sql to delete row for given key.
        :param config:      scope configuration
        :param key:         uniq key for the row, maybe `None` to delete all rows in the table, be careful
        """

class SimpleSqlBuilder(SqlBuilder, ABC):
    """
    A simple implement of SqlBuilder.
    """

    list_parameter = False      # pass parameters as a list

    def build_create_table_operation(self, *configs: RelationalDbScopeConfig, check_exists=True) -> Operation:
        def gen_column_def(col: str, config: RelationalDbScopeConfig) -> str:
            s = f"{col} {config.get_column_type(col)}"
            if col == config.uniq_column:
                s += ' PRIMARY KEY'
            return s
        def gen_column_defines(config: RelationalDbScopeConfig):
            return ','.join(gen_column_def(x, config) for x in config.columns_with_id)
        def gen_create_table_sql(config: RelationalDbScopeConfig) -> str:
            sql = "CREATE TABLE"
            if check_exists:
                sql += " IF NOT EXISTS"
            sql += f" {config.table} ({gen_column_defines(config)})"
            return sql
        lines = [gen_create_table_sql(x) + ";" for x in configs]
        return Operation(statement="\n".join(lines), many=len(configs) > 1)

    @abstractmethod
    def get_variable_placeholder(self, name: str = None, order: int = None, value: Any = None) -> str:
        """
        Get variable placeholder in operation template.
        :param name:                variable name, used for named and pyformat style
        :param order:               order in the parameters, used for numeric style, start from 1
        :param value:               variable value, for ansi c printf format codes without '%', such as 's' or 'f'
                                    used for format and pyformat style
        """

    def config_variable(self, name: str = None, order: int = None, value: Any = None,
                        variable_mapping: Dict[str, Any] = None) -> str:
        """
        Configure a variable with name, order, value.
        :param name:                variable name, used for named and pyformat style
        :param order:               order in the parameters, used for numeric style, start from 1
        :param value:               variable value, for ansi c printf format codes without '%', such as 's' or 'f'
                                    used for format and pyformat style
        :param variable_mapping:    name -> value, used to save value to variable in this method
        :return:                    a placeholder in operation template
        """
        if variable_mapping is not None:
            variable_mapping[name] = value
        return self.get_variable_placeholder(name=name, order=order, value=value)

    def normalize_variable_values(self, variable_values: Dict[str, Any],
                                  variable_names: Sequence[str] = None) -> _EXECUTE_PARAM_TYPE:
        """
        All parameters are saved in a dict first, in this method, it can be changed to a list.
        Element in variable_names should be included in variable_values.
        """
        if self.list_parameter:
            if variable_names is not None:
                return [variable_values[x] for x in variable_names]
            return list(variable_values.values())
        return variable_values

    def build_select_operation(self, config: RelationalDbScopeConfig, key: _KEY_TYPE = None, limit: int = -1,
                               select_columns: Sequence[str] = None) -> Operation:
        if select_columns is None:
            select_columns = config.columns_with_id
        elif isinstance(select_columns, str):
            select_columns = [select_columns]
        stmt = f"SELECT {','.join(select_columns)} FROM {config.scope}"
        variable_values = {}
        if key is not None:
            name = config.uniq_column
            placeholder = self.config_variable(name=name, order=1, value=key, variable_mapping=variable_values)
            stmt += f" WHERE {config.uniq_column} = {placeholder}"
        if limit > 0:
            stmt += f" LIMIT {limit}"
        return Operation(statement=stmt, parameters=self.normalize_variable_values(variable_values))

    def _gen_update_statement(self, config, value: Dict[str, Any]) -> Tuple[str, bool]:
        """
        Generate statement for Operation, data of uniq column have added to value.
        :return:    (statement, many or not)
        """
        # sql is writen based of syntax of sqlite, maybe is incapable with other database
        first = ','.join(self.config_variable(name=k, order=i, value=v)
                         for i, (k, v) in enumerate(value.items(), 1))
        second = ','.join(f'{k}={self.config_variable(name=k, order=i, value=v)}'
                          for i, (k, v) in enumerate(value.items(), 1))
        return (f"INSERT INTO {config.scope}({','.join(value.keys())})"
                f" VALUES ({first})"
                f" ON CONFLICT({config.uniq_column}) DO UPDATE"
                f" SET {second}"), False

    def _gen_update_variables(self, value: Dict[str, Any]) -> Tuple[Dict[str, Any], Sequence[str]]:
        """
        Generate variable values and name list used when execute().
        :param value:   value with uniq column
        :return:    (variable_values, variable_name_list)
        """
        return value, list(value.keys()) * 2

    def build_update_operation(self, config: RelationalDbScopeConfig,
                               key: _KEY_TYPE, value: Dict[str, Any]) -> Operation:
        # if uniq column not in value, add it
        if config.uniq_column not in value:
            value = dict(value)
            value[config.uniq_column] = key

        # gen statement and parameters
        stmt, many = self._gen_update_statement(config=config, value=value)
        variable_values, variable_names = self._gen_update_variables(value)
        parameters = self.normalize_variable_values(variable_values, variable_names)

        return Operation(statement=stmt, parameters=parameters, many=many)

    def build_delete_operation(self, config: RelationalDbScopeConfig, key: _KEY_TYPE = None) -> Operation:
        stmt = f"DELETE FROM {config.scope}"
        variable_values = {}
        if key is not None:
            placeholder = self.config_variable(name=config.uniq_column, order=1, value=key,
                                               variable_mapping=variable_values)
            stmt += f" WHERE {config.uniq_column} = {placeholder}"
        return Operation(statement=stmt, parameters=self.normalize_variable_values(variable_values))

class QmarkSqlBuilder(SimpleSqlBuilder):

    list_parameter = True

    def get_variable_placeholder(self, name: str = None, order: int = None, value: Any = None) -> str:
        return "?"

class NumericSqlBuilder(SimpleSqlBuilder):

    list_parameter = True

    def get_variable_placeholder(self, name: str = None, order: int = None, value: Any = None) -> str:
        return f":{order}"

class NamedSqlBuilder(SimpleSqlBuilder):

    list_parameter = False

    def get_variable_placeholder(self, name: str = None, order: int = None, value: Any = None) -> str:
        return f":{name}"

class FormatSqlBuilder(SimpleSqlBuilder):

    list_parameter = True

    def get_value_type(self, value: Any) -> str:
        if isinstance(value, str):
            return 's'
        if isinstance(value, int):
            return 'd'
        if isinstance(value, float):
            return 'f'
        return 's'

    def get_variable_placeholder(self, name: str = None, order: int = None, value: Any = None) -> str:
        return f"%{self.get_value_type(value)}"

class PyformatSqlBuilder(FormatSqlBuilder):

    list_parameter = False

    def get_variable_placeholder(self, name: str = None, order: int = None, value: Any = None) -> str:
        return f"%({name}){self.get_value_type(value)}"

# paramstyle -> builder
DEFAULT_SQL_BUILDERS = {
    "qmark": QmarkSqlBuilder(),
    "numeric": NumericSqlBuilder(),
    "named": NamedSqlBuilder(),
    "format": FormatSqlBuilder(),
    "pyformat": PyformatSqlBuilder(),
}

class RelationalDbCache(Cache, ABC):
    """
    Use a relational database as backend.
    Each scope corresponds to a table, and each key corresponds to a row.
    It's recommended to use a string as key, but other type such as int is also allowed.
    """

    # if given api_module, use the module to connect
    api_module: ModuleType

    # parameter marker format described in pep-0249
    paramstyle: Literal['qmark', 'numeric', 'named', 'format', 'pyformat']

    # how to build fetch, update and delete sql
    # all sql required by cache should be generated from the builder
    sql_builder: SqlBuilder

    def __init_subclass__(cls):
        super().__init_subclass__()

        # if paramstyle not specified, use value in module
        if ((not hasattr(cls, 'paramstyle') or cls.paramstyle is None)
                and (hasattr(cls, 'api_module') and cls.api_module is not None)):
            cls.paramstyle = cls.api_module.paramstyle

        # if sql_builder not specified, use default
        if ((not hasattr(cls, 'sql_builder') or cls.sql_builder is None)
                and (hasattr(cls, 'paramstyle') and cls.paramstyle is not None)):
            if cls.paramstyle not in DEFAULT_SQL_BUILDERS:
                raise ValueError(f"Wrong paramstyle: {cls.paramstyle}")
            cls.sql_builder = DEFAULT_SQL_BUILDERS[cls.paramstyle]

    def connect_db(self, database: str = None, connect_args: Dict[str, Any] = None):
        """
        Connect to a database, return a connection object described in pep-0249.

        If always given a connection when init, this method may never run.
        """
        return self.api_module.connect(database, **(connect_args or {}))

    def __init__(self, connection=None, database: str = None, connect_args: Dict[str, Any] = None,
                 scopes: List[RelationalDbScopeConfig] = None,
                 new_scope_config: Callable[[str], RelationalDbScopeConfig] = None,
                 all_table_as_scope=True):
        """
        Create a cache based on relational database.
        :param connection:          connection to the database
        :param database:            uri for the database
        :param connect_args:        custom connect args
        :param scopes:              initialized scopes
        :param new_scope_config:    when a new scope given, how to config it
        :param all_table_as_scope:  if `True`, add all table in database to scopes
        """
        self.conn = connection or self.connect_db(database, connect_args)
        self.cursor = self.conn.cursor()
        self._scopes = {}   # scope -> config
        self._tables = {}   # table -> config, should correspond to _scopes
        self._new_scope_config = new_scope_config
        self._init_scopes(scopes=scopes, all_table_as_scope=all_table_as_scope)

    def close(self):
        self.cursor.close()
        self.conn.close()

    def _add_scope(self, config: RelationalDbScopeConfig):
        # scope and table should be uniq
        if config.scope in self._scopes:
            raise ValueError(f"duplicated scope: {config.scope}")
        if config.table in self._tables:
            raise ValueError(f"duplicated table: {config.table}")
        self._scopes[config.scope] = config
        self._tables[config.table] = config

    def _create_table_if_not_exists(self, *scopes: RelationalDbScopeConfig):
        operations = [self.sql_builder.build_create_table_operation(config, check_exists=True)
                      for config in scopes]
        self._execute(*operations, cursor='new', commit=True)

    def _get_all_table_names_in_db(self) -> List[str]:
        """
        Get all tables in the database, used when init scopes.
        """
        operation = self.sql_builder.build_select_all_table_operation()
        # assume first is table name
        return [x[0] for x in self._execute(operation, fetch_size='all')]

    def _get_table_columns(self, table: str) -> List[Tuple[Any]]:
        """
        Get columns for given table.
        :return:    column names
        """
        operation = self.sql_builder.build_select_table_columns_operation(table)
        columns = self._execute(operation, fetch_size='all')
        return list(columns)

    def _get_table_column_names(self, table: str) -> List[str]:
        """
        Get column names for given table.
        """
        columns = self._get_table_columns(table)
        # assume first result is column name.
        return [x[0] for x in columns]

    def _get_unique_column_name(self, table: str) -> str:
        """
        Get uniq column name for given table.
        """
        operation = self.sql_builder.build_select_table_columns_operation(table, filter_uniq=True)
        columns = self._execute(operation, fetch_size='all')
        if len(columns) != 1:
            raise ValueError(f"There should be exactly one uniq column, but {len(columns)} has given.")
        # assume first result is column name.
        return columns[0][0]

    def _init_scopes(self, scopes: List[RelationalDbScopeConfig], all_table_as_scope: bool):
        # if some scopes have given, add to the scope mapper
        # if table not exists, create it
        if scopes:
            for config in scopes:
                self._add_scope(config)
            self._create_table_if_not_exists(*scopes)

        # find tables in database, if table not configured, add to the scope mapper
        if all_table_as_scope:
            # select all tables
            tables = self._get_all_table_names_in_db()

            # table should not bound to a scope, and should not be same as a scope, remove these tables
            tables = [x[0] for x in tables if x[0] not in self._tables and x[0] not in self._scopes]

            # for retain tables, add to the cache
            for table in tables:
                columns = self._get_table_columns(table)
                uniq_column = self._get_unique_column_name(table)
                self._add_scope(RelationalDbScopeConfig(
                    scope=table,    # use table name as scope
                    table=table,
                    uniq_column=uniq_column,     # uniq column as id
                    columns=columns,
                ))

    def _get_scope_config(self, scope: str) -> RelationalDbScopeConfig:
        if scope in self._scopes:
            return self._scopes[scope]
        if not self._new_scope_config:
            raise ValueError(f"new scope is not allowed: {scope}")
        config = self._new_scope_config(scope)
        if scope != config.scope:
            raise ValueError(f"conflict scope: {scope} {config.scope}")
        self._add_scope(config)
        self._create_table_if_not_exists(config)    # create table if not exists
        return config

    def _fetch_data(self, cursor, size: _FETCH_SIZE_TYPE = None) -> Any:
        if size is None:
            size = 0
        elif size == 'one':
            size = 1
        elif size == 'all':
            size = -1
        if not isinstance(size, int):
            raise ValueError(f"Unexpected size type: {type(size)}")
        if size == 0:
            return None
        elif size == 1:
            return cursor.fetchone()
        elif size < 0:
            return cursor.fetchall()
        else:
            return cursor.fetchmany(size)

    def _execute_many0(self, cursor, operation: Operation):
        """
        Some api not allow executemany(), in this case, overwrite this method.
        """
        cursor.executemany(operation.statement, operation.parameters)

    def _execute(self, *operations: Union[str, Operation], cursor=None, auto_close_cursor=False,
                 fetch_size: _FETCH_SIZE_TYPE = None, commit=False) -> Any:
        """
        Execute sql and fetch result.
        :param operations:      sequence of operation, same as described in pep-0249
        :param cursor:          if given, use it; if 'new', create a new cursor; if not given, use global
        :param auto_close_cursor:   close cursor in the end, should not set when fetch data
        :param fetch_size:      how many rows should return
        :param commit:          do commit to database or not
        """
        if cursor is None:
            cursor = self.cursor
        elif cursor == 'new':
            cursor = self.conn.cursor()
        for operation in operations:
            if isinstance(operation, str):
                operation = Operation(statement=operation)
            if operation.many:
                self._execute_many0(cursor, operation)
            else:
                cursor.execute(operation.statement, operation.parameters)
        result = self._fetch_data(cursor, size=fetch_size)
        if commit:
            self.conn.commit()
        if cursor == 'new' and auto_close_cursor:
            # close the cursor if it's created in this method
            cursor.close()
        return result

    def _check_key(self, key: _KEY_TYPE):
        if key is None:
            raise ValueError("Arg key should not be None.")

    def exists(self, key: _KEY_TYPE, scope: str = None, **kwargs) -> bool:
        self._check_key(key)
        config = self._get_scope_config(scope)
        operation = self.sql_builder.build_select_operation(config=config, key=key, limit=1)
        return self._execute(operation, fetch_size=1) is not None

    def fetch(self, key: _KEY_TYPE, default: Any = None, scope: Any = None, **kwargs) -> Optional[Dict[str, Any]]:
        self._check_key(key)
        config = self._get_scope_config(scope)
        operation = self.sql_builder.build_select_operation(config=config, key=key, limit=1)
        row = self._execute(operation, fetch_size=1)
        if row is None:
            return default
        return dict(zip(config.columns, row))

    def set(self, key: _KEY_TYPE, value: Dict[str, Any], scope: Any = None, **kwargs) -> bool:
        self._check_key(key)
        config = self._get_scope_config(scope)
        if config.uniq_column in value:
            if key != value[config.uniq_column]:
                raise ValueError(f"key {key} is different from value {value[config.uniq_column]}")
            row = {name: value[name] for name in config.columns_with_id if name in value}
        else:
            row = {config.uniq_column: key}
            row.update((name, value[name]) for name in config.columns if name in value)
        operation = self.sql_builder.build_update_operation(config=config, key=key, value=row)
        self._execute(operation, cursor='new', auto_close_cursor=True, commit=True)
        return True

    def pop(self, key: _KEY_TYPE, scope: str = None, **kwargs) -> None:
        self._check_key(key)
        config = self._get_scope_config(scope)
        operation = self.sql_builder.build_delete_operation(config=config, key=key)
        self._execute(operation, cursor='new', auto_close_cursor=True, commit=True)

    def scopes(self) -> Iterable[str]:
        return self._scopes.keys()

    def keys(self, scope: str = None) -> Iterable[str]:
        config = self._get_scope_config(scope)
        operation = self.sql_builder.build_select_operation(config=config, key=None)
        return self._execute(operation, fetch_size='all', cursor='new')
