# -*- coding: utf-8 -*-

"""
Relational database can be used as a cache or a persistence storage.
Actually, implement a cache is enough.
"""

import collections
from typing import (
    Any, Iterable, Dict,
    List, Type, Optional,
    Sequence, Callable,
    Literal, Union,
    Tuple, Hashable,
)
import re
from abc import ABC, abstractmethod
from types import ModuleType
import dataclasses
import warnings
from ..core import RegistrableCache


_FETCH_SIZE_TYPE = Union[Literal['one', 'all'], int]
_EXECUTE_PARAM_TYPE = Union[Sequence[Any], Dict[str, Any]]
_COLUMN_VALUE_TYPE = Union[str, int]
_KEY_TYPE = Union[_COLUMN_VALUE_TYPE, Sequence[_COLUMN_VALUE_TYPE]]


# cache data format may diff from db api, we need an adapter to bridging
class ValueAdapter(ABC):
    """
    Method `build_column_values` and `parse_column_values` should be reversed one-to-one mapping.
    That is to say:
        parse_column_values(build_column_values(x)) == x
        build_column_values(parse_column_values(x)) == x
    """
    @abstractmethod
    def build_column_values(self, value: Any) -> Dict[str, Any]:
        """
        Used in method `RelationalDbCache.set()` to build column values
        """
    @abstractmethod
    def parse_column_values(self, value: Dict[str, Any]) -> Any:
        """
        Used in method `RelationalDbCache.fetch()` to parse column values.
        """

class DefaultAdapter(ValueAdapter):
    """
    Cache value should be exactly column values and thus nothing should do.
    """

    def build_column_values(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return value

    def parse_column_values(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return value

class SingleAdapter(ValueAdapter):
    """
    Cache value is exactly one column.
    """

    def __init__(self, col: str):
        self.col = col

    def build_column_values(self, value: Any) -> Dict[str, Any]:
        return {self.col: value}

    def parse_column_values(self, value: Dict[str, Any]) -> Any:
        return value[self.col]

class SequenceAdapter(ValueAdapter):
    """
    Cache value is a list or tuple, corresponding to some columns.
    """

    def __init__(self, cols: Sequence[str], return_type: Type[Union[list, tuple]] = tuple):
        self.cols = cols
        self.return_type = return_type

    def build_column_values(self, value: Sequence[Any]) -> Dict[str, Any]:
        return dict(zip(self.cols, value))

    def parse_column_values(self, value: Dict[str, Any]) -> Sequence[Any]:
        return self.return_type(map(value.get, self.cols))


@dataclasses.dataclass
class Operation:
    """
    define an operation for execute() or executemany()
    """
    statement: str       # arg operation for cursor.execute()
    parameters: _EXECUTE_PARAM_TYPE = dataclasses.field(default_factory=list)  # arg parameters for cursor.execute()

    # many statements in template or not, if True, use cursor.executemany() instead of cursor.execute()
    many: bool = False


class BaseTableConfig:
    def __init__(self, table: str,
                 uniq_columns: Sequence[str] = ('id',),
                 columns: Sequence[str] = ('data',),
                 column_types: Dict[str, str] = None,
                 value_adapter: Union[Literal['auto', 'default', 'single', 'tuple', 'list'], ValueAdapter] = 'auto',
                 default_column_type: str = None):
        """
        :param table:
        :param uniq_columns:    unique columns to identify rows
        :param columns:         columns to select when invoke `cache.fetch()`
        :param column_types:    if column not specified here, type should be str
        :param value_adapter:   convert value when fetch and set
        :param default_column_type:     if column type not specified, use this value as default
        """
        self.table = table
        self.columns = columns
        self.column_types = dict(column_types or {})
        self.default_column_type = default_column_type
        if isinstance(uniq_columns, str):
            self.uniq_columns = (uniq_columns,)
        else:
            self.uniq_columns = uniq_columns
        if not self.uniq_columns:
            raise ValueError("uniq_columns cannot be empty.")
        self.columns_with_id = [x for x in self.uniq_columns if x not in self.columns] + list(self.columns)
        self.value_adapter = self._check_value_adapter(value_adapter, columns)

    @staticmethod
    def _check_value_adapter(adapter, columns: Sequence[str]) -> ValueAdapter:
        if adapter == 'auto':
            if len(columns) == 1:
                return SingleAdapter(columns[0])
            return DefaultAdapter()
        elif adapter == 'default':
            return DefaultAdapter()
        elif adapter == 'single':
            if len(columns) != 1:
                raise ValueError(f"columns length should be 1 when value_adapter is 'single'.")
            return SingleAdapter(columns[0])
        elif adapter == 'tuple':
            return SequenceAdapter(columns, return_type=tuple)
        elif adapter == 'list':
            return SequenceAdapter(columns, return_type=list)

        if isinstance(adapter, ValueAdapter):
            return adapter

        if isinstance(adapter, str):
            msg = f"Unexpected value_adapter: {adapter}"
        else:
            msg = f"Unexpected value_adapter type: {type(adapter)}"
        raise ValueError(msg)

    def get_column_type(self, col: str) -> str:
        return self.column_types.get(col, self.default_column_type)

    @staticmethod
    def normalize_columns_values(value: Any, columns: Sequence[str]) -> Sequence[Any]:
        n = len(columns)
        if n > 1:
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"value should be a list or tuple, got {type(value)}")
            m = len(value)
            if m != n:
                raise ValueError(f"value size should be {n}, but got {m}")
            return value
        else:
            return [value]

    def normalize_uniq_column_values(self, key: Any) -> Sequence[Any]:
        return self.normalize_columns_values(key, self.uniq_columns)


# sql syntax may diff for different db, each backend should implement it's sql_builder
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

    def get_column_name_from_result(self, result: Sequence[Any]) -> str:
        """
        Get column name from result of operation generated by `build_select_table_columns_operation()`.
        """
        # assume first result is column name.
        return result[0]

    @abstractmethod
    def build_create_table_operation(self, *configs: BaseTableConfig, check_exists=True):
        """
        Generate sql to create a table.
        """

    @abstractmethod
    def build_select_operation(self, config: BaseTableConfig, key: Sequence[Any] = None, limit: int = -1,
                               select_columns: Sequence[str] = None, distinct=False) -> Operation:
        """
        Generate sql to select row for given key.
        :param config:          scope configuration
        :param key:             uniq key for the row, maybe `None` to select all rows in the table
        :param limit:           limit number
        :param select_columns:  columns to be selected, if not set, select all in config
        :param distinct:        whether distinct select columns
        """

    @abstractmethod
    def build_update_operation(self, config: BaseTableConfig, key: Sequence[Any], value: Dict[str, Any]) -> Operation:
        """
        Generate sql to update or insert row for given key with given value.
        :param config:      scope configuration
        :param key:         uniq key for the row
        :param value:       column values for given uniq key
        """

    @abstractmethod
    def build_delete_operation(self, config: BaseTableConfig, key: Sequence[Any] = None) -> Operation:
        """
        Generate sql to delete row for given key.
        :param config:      scope configuration
        :param key:         uniq key for the row, maybe `None` to delete all rows in the table, be careful
        """

class SimpleSqlBuilder(SqlBuilder, ABC):
    """
    A simple implement of SqlBuilder.
    """

    # whether to pass parameters as a list
    list_parameter = False

    # if column type not specified in the config, use this value as default
    # should be overwritten for different backends
    default_column_type = 'text'

    def _get_column_type(self, col: str, config: BaseTableConfig) -> str:
        return config.get_column_type(col) or self.default_column_type

    def _gen_column_def(self, col: str, config: BaseTableConfig) -> str:
        t = self._get_column_type(col, config)
        s = f"{col} {t}"
        # if col in config.uniq_columns:
            # s += " UNIQUE"
            # s += ' PRIMARY KEY'
        return s

    def _gen_column_defines(self, config: BaseTableConfig):
        columns = [self._gen_column_def(x, config) for x in config.columns_with_id]
        columns += [f" PRIMARY KEY ({','.join(config.uniq_columns)})"]
        return ','.join(columns)

    def _gen_create_table_sql(self, config: BaseTableConfig, check_exists=True) -> str:
        sql = "CREATE TABLE"
        if check_exists:
            sql += " IF NOT EXISTS"
        sql += f" {config.table} ({self._gen_column_defines(config)})"
        return sql

    def build_create_table_operation(self, *configs: BaseTableConfig, check_exists=True) -> Operation:
        lines = [self._gen_create_table_sql(x) + ";" for x in configs]
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

    def build_select_operation(self, config: BaseTableConfig, key: Sequence[Any] = None, limit: int = -1,
                               select_columns: Sequence[str] = None, distinct=False) -> Operation:
        if select_columns is None:
            # only select configured columns; if id not configured, ignore it
            select_columns = config.columns
        elif isinstance(select_columns, str):
            select_columns = [select_columns]
        stmt = "SELECT"
        if distinct:
            stmt += f" DISTINCT"
        stmt += f" {','.join(select_columns)} FROM {config.table}"
        variable_values = collections.OrderedDict()
        if key is not None:
            assert len(config.uniq_columns) == len(key)
            condition = []
            for i, (name, k) in enumerate(zip(config.uniq_columns, key), 1):
                placeholder = self.config_variable(name=name, order=i, value=k, variable_mapping=variable_values)
                condition.append(f"{name} = {placeholder}")
            condition = ' AND '.join(condition)
            stmt += f" WHERE {condition}"
        if limit > 0:
            stmt += f" LIMIT {limit}"
        return Operation(statement=stmt, parameters=self.normalize_variable_values(variable_values))

    def _gen_update_statement(self, config: BaseTableConfig, value: Dict[str, Any]) -> Tuple[str, bool]:
        """
        Generate statement for Operation, data of uniq column have added to value.
        :return:    (statement, many or not)
        """
        # sql is writen based of syntax of sqlite, maybe is incapable with other database
        first = ','.join(self.config_variable(name=k, order=i, value=v)
                         for i, (k, v) in enumerate(value.items(), 1))
        second = ','.join(f'{k}={self.config_variable(name=k, order=i, value=v)}'
                          for i, (k, v) in enumerate(value.items(), 1))
        return (f"INSERT INTO {config.table}({','.join(value.keys())})"
                f" VALUES ({first})"
                f" ON CONFLICT({','.join(config.uniq_columns)}) DO UPDATE"
                f" SET {second}"), False

    def _gen_update_variables(self, value: Dict[str, Any]) -> Tuple[Dict[str, Any], Sequence[str]]:
        """
        Generate variable values and name list used when execute().
        :param value:   value with uniq column
        :return:    (variable_values, variable_name_list)
        """
        return value, list(value.keys()) * 2

    def build_update_operation(self, config: BaseTableConfig, key: Sequence[Any], value: Dict[str, Any]) -> Operation:
        # add uniq column to value
        assert len(config.uniq_columns) == len(key)
        value = dict(value)
        for name, k in zip(config.uniq_columns, key):
            value[name] = k

        # gen statement and parameters
        stmt, many = self._gen_update_statement(config=config, value=value)
        variable_values, variable_names = self._gen_update_variables(value)
        parameters = self.normalize_variable_values(variable_values, variable_names)

        return Operation(statement=stmt, parameters=parameters, many=many)

    def build_delete_operation(self, config: BaseTableConfig, key: Sequence[Any] = None) -> Operation:
        stmt = f"DELETE FROM {config.table}"
        variable_values = collections.OrderedDict()
        if key is not None:
            assert len(config.uniq_columns) == len(key)
            condition = []
            for i, (name, k) in enumerate(zip(config.uniq_columns, key), 1):
                placeholder = self.config_variable(name=name, order=1, value=k,
                                                   variable_mapping=variable_values)
                condition.append(f"{name} = {placeholder}")
            condition = ' AND '.join(condition)
            stmt += f" WHERE {condition}"
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


class BaseRelationalDbCache(RegistrableCache, ABC):
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

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'BaseRelationalDbCache':
        assert re.match(r'\w+://.+', uri), uri
        schema, database = uri.split('://')
        return cls(database=database, **kwargs)

    def __init__(self, connection=None, database: str = None, connect_args: Dict[str, Any] = None):
        """
        Create a cache based on relational database.
        :param connection:          connection to the database
        :param database:            uri for the database
        :param connect_args:        custom connect args
        """
        self.conn = connection or self.connect_db(database, connect_args)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def _create_table_if_not_exists(self, *scopes: BaseTableConfig):
        operations = [self.sql_builder.build_create_table_operation(config, check_exists=True)
                      for config in scopes]
        self._execute(*operations, cursor='new', commit=True)

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

    def _execute(self, *operations: Union[str, Operation], cursor='new', auto_close_cursor=True,
                 fetch_size: _FETCH_SIZE_TYPE = None, commit=False) -> Any:
        """
        Execute sql and fetch result.
        :param operations:      sequence of operation, same as described in pep-0249
        :param cursor:          if `None`, use global; if 'new', create a new cursor; else, use the given cursor
        :param auto_close_cursor:   close cursor in the end
        :param fetch_size:      how many rows should return
        :param commit:          do commit to database or not
        """
        close_cursor = False
        if cursor is None:
            cursor = self.cursor
        elif cursor == 'new':
            cursor = self.conn.cursor()
            close_cursor = auto_close_cursor
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
        if close_cursor:
            # close the cursor if it's created in this method
            cursor.close()
        return result

    def _get_all_table_names_in_db(self) -> List[str]:
        """
        Get all tables in the database, used when init scopes.
        """
        operation = self.sql_builder.build_select_all_table_operation()
        # assume first is table name
        return [x[0] for x in self._execute(operation, fetch_size='all')]

    def _get_table_columns(self, table: str) -> List[Sequence[Any]]:
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
        return [self.sql_builder.get_column_name_from_result(x) for x in columns]

    def _get_unique_columns(self, table: str) -> List[Sequence[Any]]:
        """
        Get uniq columns for given table.
        """
        operation = self.sql_builder.build_select_table_columns_operation(table, filter_uniq=True)
        columns = self._execute(operation, fetch_size='all')
        return list(columns)

    def _get_unique_column_names(self, table: str) -> List[str]:
        """
        Get uniq column names for given table.
        """
        columns = self._get_unique_columns(table)
        return [self.sql_builder.get_column_name_from_result(x) for x in columns]

    def _get_unique_column_name(self, table: str) -> str:
        """
        Get uniq column name for given table.
        """
        warnings.warn("Deprecated, use _get_unique_column_names() instead", DeprecationWarning)
        columns = self._get_unique_column_names(table)
        if len(columns) != 1:
            raise ValueError(f"There should be exactly one uniq column, but {len(columns)} has given.")
        return columns[0]

    def _exists(self, key: Sequence[Any], table: BaseTableConfig) -> bool:
        """
        Actual method to test if a key exists in the table.
        :param key:         value of uniq columns
        :param table:       table config
        """
        operation = self.sql_builder.build_select_operation(
            config=table, key=key, limit=1,
            select_columns=table.uniq_columns,
        )
        return self._execute(operation, fetch_size=1) is not None

    def _fetch(self, key: Sequence[Any], table: BaseTableConfig, default: Any = None) -> Any:
        """
        Actual method to fetch data from table.
        :param key:         value of uniq columns
        :param table:       table config
        :param default:     if select no data, return this value
        """
        operation = self.sql_builder.build_select_operation(
            config=table, key=key, limit=1,
            select_columns=table.columns,
        )
        row = self._execute(operation, fetch_size=1)
        if row is None:
            return default
        value = dict(zip(table.columns, row))
        return table.value_adapter.parse_column_values(value)

    def _set(self, key: Sequence[Any], value: Any, table: BaseTableConfig) -> bool:
        """
        Actual method to update or insert row into table.
        :param key:         value of uniq columns
        :param value:       value of all other columns
        :param table:       table config
        """
        value = table.value_adapter.build_column_values(value)
        row = {}
        for name, k in zip(table.uniq_columns, key):
            if name in value and k != value[name]:
                raise ValueError(f"column {name} key {k} is different from value {value[name]}")
            row[name] = k
        row.update((name, value[name]) for name in table.columns if name in value)
        operation = self.sql_builder.build_update_operation(config=table, key=key, value=row)
        self._execute(operation, cursor='new', auto_close_cursor=True, commit=True)
        return True

    def _pop(self, key: Sequence[Any], table: BaseTableConfig):
        """
        Actual method to remove row from table.
        :param key:         value of uniq columns
        :param table:       table config
        """
        operation = self.sql_builder.build_delete_operation(config=table, key=key)
        self._execute(operation, cursor='new', auto_close_cursor=True, commit=True)


class RelationalDbScopeConfig(BaseTableConfig):
    def __init__(self, scope: Optional[Hashable], table: str = None,
                 uniq_column: str = None,
                 uniq_columns: Sequence[str] = ('id',),
                 columns: Sequence[str] = ('data',),
                 column_types: Dict[str, str] = None,
                 value_adapter: Union[Literal['auto', 'default', 'single', 'tuple', 'list'], ValueAdapter] = 'auto',
                 default_column_type: str = None):
        self.scope = scope
        if not table:
            if not isinstance(scope, str):
                raise ValueError("Arg scope must be a str when table not given.")
            table = scope
        if uniq_column:
            warnings.warn("`uniq_column` is deprecated, use `uniq_columns` instead.", DeprecationWarning)
            uniq_columns = (uniq_column,)
        super().__init__(table, uniq_columns, columns, column_types, value_adapter, default_column_type)

class RelationalDbCache(BaseRelationalDbCache, ABC):
    """
    Use a relational database as backend.
    Each scope corresponds to a table, and each key corresponds to a row.
    It's recommended to use a string as key, but other type such as int is also allowed.
    """

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'RelationalDbCache':
        assert re.match(r'\w+://.+', uri), uri
        schema, database = uri.split('://')
        return cls(database=database, **kwargs)

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
        super().__init__(connection, database, connect_args)
        self._scopes = {}   # scope -> config
        self._tables = {}   # table -> config, should correspond to _scopes
        self._new_scope_config = new_scope_config
        self._init_scopes(scopes=scopes, all_table_as_scope=all_table_as_scope)

    def _add_scope(self, config: RelationalDbScopeConfig):
        # scope and table should be uniq
        if config.scope in self._scopes:
            raise ValueError(f"duplicated scope: {config.scope}")
        if config.table in self._tables:
            raise ValueError(f"duplicated table: {config.table}")
        self._scopes[config.scope] = config
        self._tables[config.table] = config

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
            table_names = self._get_all_table_names_in_db()

            # table should not bound to a scope, and should not be same as a scope, remove these tables
            table_names = [x for x in table_names if x not in self._tables and x not in self._scopes]

            # for retain tables, add to the cache
            for table in table_names:
                columns = self._get_table_column_names(table)
                uniq_columns = self._get_unique_column_names(table)
                self._add_scope(RelationalDbScopeConfig(
                    scope=table,    # use table name as scope
                    table=table,
                    uniq_columns=uniq_columns,     # uniq column as id
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

    def _check_key(self, key: _KEY_TYPE):
        if key is None:
            raise ValueError("Arg key should not be None.")

    def exists(self, key: _KEY_TYPE, scope: str = None, **kwargs) -> bool:
        self._check_key(key)
        config = self._get_scope_config(scope)
        key = config.normalize_uniq_column_values(key)
        return self._exists(key, config)

    def fetch(self, key: _KEY_TYPE, default: Any = None, scope: Any = None, **kwargs) -> Any:
        self._check_key(key)
        config = self._get_scope_config(scope)
        key = config.normalize_uniq_column_values(key)
        return self._fetch(key, config, default=default)

    def set(self, key: _KEY_TYPE, value: Any, scope: Any = None, **kwargs) -> bool:
        self._check_key(key)
        config = self._get_scope_config(scope)
        key = config.normalize_uniq_column_values(key)
        return self._set(key, value, config)

    def pop(self, key: _KEY_TYPE, scope: str = None, **kwargs) -> None:
        self._check_key(key)
        config = self._get_scope_config(scope)
        key = config.normalize_uniq_column_values(key)
        self._pop(key, config)

    def scopes(self) -> Iterable[str]:
        return self._scopes.keys()

    def keys(self, scope: str = None) -> Iterable[str]:
        config = self._get_scope_config(scope)
        operation = self.sql_builder.build_select_operation(config=config, key=None)
        return self._execute(operation, fetch_size='all', cursor='new')


class SingleTableConfig(BaseTableConfig):
    def __init__(self, table: str,
                 scope_columns: Sequence[str] = ('scope',),
                 key_columns: Sequence[str] = ('id',),
                 columns: Sequence[str] = ('data',),
                 column_types: Dict[str, str] = None,
                 value_adapter: Union[Literal['auto', 'default', 'single', 'tuple', 'list'], ValueAdapter] = 'auto',
                 default_column_type: str = None):
        self.scope_columns = tuple(scope_columns)
        self.key_columns = tuple(key_columns)
        uniq_columns = self.scope_columns + self.key_columns
        super().__init__(table, uniq_columns, columns, column_types, value_adapter, default_column_type)

    def normalize_scope_column_values(self, scope: Any) -> Sequence[Any]:
        return self.normalize_columns_values(scope, self.scope_columns)

    def normalize_key_column_values(self, scope: Any) -> Sequence[Any]:
        return self.normalize_columns_values(scope, self.key_columns)

class SingleTableCache(BaseRelationalDbCache):
    """
    Use single table in the db as backend.
    This is useful when data of different scopes stored in the same table.
    """

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'SingleTableCache':
        assert re.match(r'\w+://.+', uri), uri
        schema, database = uri.split('://')
        return cls(database=database, **kwargs)

    def __init__(self, connection=None, database: str = None, connect_args: Dict[str, Any] = None,
                 config: SingleTableConfig = None):
        """
        Create a cache based on relational database.
        :param connection:          connection to the database
        :param database:            uri for the database
        :param connect_args:        custom connect args
        :param config:              config columns for scope and uniq columns
        """
        super().__init__(connection, database, connect_args)
        self.config = config

    def _check_key(self, key: _KEY_TYPE, scope: _KEY_TYPE):
        if key is None:
            raise ValueError("Arg key should not be None.")
        if scope is None:
            raise ValueError("Arg scope should not be None.")

    def _gen_uniq_column_values(self, key: _KEY_TYPE, scope: _KEY_TYPE) -> Sequence[Any]:
        self._check_key(key, scope)
        key = self.config.normalize_key_column_values(key)
        scope = self.config.normalize_scope_column_values(scope)
        return tuple(scope) + tuple(key)

    def exists(self, key: _KEY_TYPE, scope: _KEY_TYPE = None, **kwargs) -> bool:
        uniq_column_values = self._gen_uniq_column_values(key, scope)
        return self._exists(uniq_column_values, self.config)

    def fetch(self, key: _KEY_TYPE, default: Any = None, scope: _KEY_TYPE = None, **kwargs) -> Any:
        uniq_column_values = self._gen_uniq_column_values(key, scope)
        return self._fetch(uniq_column_values, self.config, default=default)

    def set(self, key: _KEY_TYPE, value: Any, scope: _KEY_TYPE = None, **kwargs) -> Any:
        uniq_column_values = self._gen_uniq_column_values(key, scope)
        return self._set(uniq_column_values, value, self.config)

    def pop(self, key: _KEY_TYPE, scope: _KEY_TYPE = None, **kwargs) -> Any:
        uniq_column_values = self._gen_uniq_column_values(key, scope)
        return self._pop(uniq_column_values, self.config)

    def scopes(self) -> Iterable[Any]:
        config = self.config
        columns = config.scope_columns
        operation = self.sql_builder.build_select_operation(config, select_columns=columns, distinct=True)
        row = self._execute(operation, fetch_size='all')
        if row is None:
            return []
        if len(columns) == 1:
            return [x[0] for x in row]
        return [tuple(x) for x in row]

    def keys(self, scope: _KEY_TYPE = None) -> Iterable[Any]:
        scope = self.config.normalize_scope_column_values(scope)
        config = self.config
        columns = config.key_columns
        operation = self.sql_builder.build_select_operation(config, key=scope, select_columns=columns)
        row = self._execute(operation, fetch_size='all')
        if row is None:
            return []
        if len(columns) == 1:
            return [x[0] for x in row]
        return [tuple(x) for x in row]

