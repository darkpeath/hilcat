from typing import (
    Any, Dict, List,  Sequence, Literal, Iterable,
    Union, Callable,
)
import re
from abc import ABC
from types import ModuleType
import warnings
from hilcat.core import RegistrableCache
from .config import Operation, BaseTableConfig, RelationalDbScopeConfig, SingleTableConfig
from .sql import SqlBuilder, DEFAULT_SQL_BUILDERS

_FETCH_SIZE_TYPE = Union[Literal['one', 'all'], int]
_COLUMN_VALUE_TYPE = Union[str, int]
_KEY_TYPE = Union[_COLUMN_VALUE_TYPE, Sequence[_COLUMN_VALUE_TYPE]]

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
        self._database = database
        self._connect_args = connect_args
        self._conn = connection
        self._cursor = None

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.connect_db(self._database, self._connect_args)
        return self._conn

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.conn.cursor()
        return self._cursor

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
        if self._conn is not None:
            self._conn.close()

    def _create_table_if_not_exists(self, *tables: BaseTableConfig):
        operations = [self.sql_builder.build_create_table_operation(config, check_exists=True)
                      for config in tables]
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
        return table.decoder(value)

    def _set(self, key: Sequence[Any], value: Any, table: BaseTableConfig) -> bool:
        """
        Actual method to update or insert row into table.
        :param key:         value of uniq columns
        :param value:       value of all other columns
        :param table:       table config
        """
        value = table.encoder(value)
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

_SCOPES_ARG_TYPE = Union[
    None,
    str,    # single scope cache
    RelationalDbScopeConfig,
    Dict[str, Union[
        str,        # specify table name
        Dict[str, Any],
        RelationalDbScopeConfig
    ]],
    Sequence[Union[
        str,    # only specify scope
        Dict[str, Any],
        RelationalDbScopeConfig
    ]],
]

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

    def __init__(
        self, connection=None, database: str = None, connect_args: Dict[str, Any] = None,
        scopes: _SCOPES_ARG_TYPE = None,
        new_scope_config: Callable[[str], RelationalDbScopeConfig] = None,
        all_table_as_scope=False
    ):
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
        self._init_scopes(scopes=self._process_scopes_arg(scopes), all_table_as_scope=all_table_as_scope)

    def _process_scopes_arg(self, scopes: _SCOPES_ARG_TYPE) -> List[RelationalDbScopeConfig]:
        """
        Process scopes arg given in __init__ function as a config list.
        """
        if scopes is None:
            return []
        elif isinstance(scopes, str):
            return [RelationalDbScopeConfig(scopes)]
        elif isinstance(scopes, RelationalDbScopeConfig):
            return [scopes]
        elif isinstance(scopes, dict):
            result = []
            for scope, config in scopes.items():
                if isinstance(config, str):
                    config = RelationalDbScopeConfig(scope, table=config)
                elif isinstance(config, dict):
                    if 'scope' in config and config['scope'] != scope:
                        raise ValueError(f"conflict scope: {scope} {config['scope']}")
                    config.pop('scope', None)
                    config = RelationalDbScopeConfig(scope, **config)
                if not isinstance(config, RelationalDbScopeConfig):
                    raise ValueError(f"Invalid scope config type: {type(config)}")
                if config.scope != scope:
                    raise ValueError(f"conflict scope: {scope} {config.scope}")
                result.append(config)
            return result
        else:
            result = []
            for config in scopes:
                if isinstance(config, str):
                    config = RelationalDbScopeConfig(config)
                elif isinstance(config, dict):
                    config = RelationalDbScopeConfig(**config)
                if not isinstance(config, RelationalDbScopeConfig):
                    raise ValueError(f"Invalid scope config type: {type(config)}")
                result.append(config)
            return result

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
        config = self._process_scopes_arg({scope: config})[0]
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
        self._create_table_if_not_exists(config)

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
        select_columns = config.key_columns
        condition_columns = config.scope_columns
        operation = self.sql_builder.build_select_operation(
            config, key=scope,
            select_columns=select_columns,
            condition_columns=condition_columns
        )
        row = self._execute(operation, fetch_size='all')
        if row is None:
            return []
        if len(select_columns) == 1:
            return [x[0] for x in row]
        return [tuple(x) for x in row]

