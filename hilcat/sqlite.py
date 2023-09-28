# -*- coding: utf-8 -*-

"""
Sqlite can be used as a cache or a persistence storage.
Actually, implement a cache is enough.
"""

from typing import (
    Any, Iterable, Dict,
    Optional, List,
    Sequence, Callable,
)
import dataclasses
import sqlite3
import json
from .core import Cache

@dataclasses.dataclass
class SqliteScopeConfig:
    scope: str
    table: str = None
    uniq_id: str = 'id'
    columns: Sequence[str] = ('id', 'data')
    columns_with_id: List[str] = dataclasses.field(init=False)

    # if column not specified here, type should be str
    column_types: Dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if self.uniq_id in self.columns:
            self.columns_with_id = list(self.columns)
        else:
            self.columns_with_id = [self.uniq_id] + list(self.columns)
        if not self.table:
            self.table = self.scope

    def get_column_type(self, col: str) -> str:
        return self.column_types.get(col, 'str')

class SqliteCache(Cache):
    """
    Use a sqlite database as backend.
    Each scope corresponds to a table, and each key corresponds to a row.
    It's recommended to use a string as key, but other type such as int is also allowed.
    """

    def __init__(self, database, connect_args: Dict[str, Any] = None,
                 scopes: List[SqliteScopeConfig] = None,
                 new_scope_config: Callable[[str], SqliteScopeConfig] = None,
                 all_table_as_scope=True):
        """
        Create a cache based on sqlite.
        :param database:            a xx.db file
        :param connect_args:        custom connect args
        :param scopes:              initialed scopes
        :param new_scope_config:    if a new scope given, how to config it
        :param all_table_as_scope:  if `True`, add all table in database to scopes
        """
        self.conn = sqlite3.connect(database, **(connect_args or {}))
        self.cursor = self.conn.cursor()
        self._scopes = {}   # scope -> config
        self._tables = {}   # table -> config, should correspond to _scopes
        self._new_scope_config = new_scope_config
        self._init_scopes(scopes=scopes, all_table_as_scope=all_table_as_scope)

    def close(self):
        self.cursor.close()
        self.conn.close()

    def _add_scope(self, config: SqliteScopeConfig):
        # scope and table should be uniq
        if config.scope in self._scopes:
            raise ValueError(f"duplicated scope: {config.scope}")
        if config.table in self._tables:
            raise ValueError(f"duplicated table: {config.table}")
        self._scopes[config.scope] = config
        self._tables[config.table] = config

    def _create_table_if_not_exists(self, *scopes: SqliteScopeConfig):
        def gen_column_def(col: str, config: SqliteScopeConfig) -> str:
            s = f"{col} {config.get_column_type(col)}"
            if col == config.uniq_id:
                s += ' PRIMARY KEY'
            return s
        def gen_column_defines(config: SqliteScopeConfig):
            return ','.join(gen_column_def(x, config) for x in config.columns_with_id)
        cursor = self.conn.cursor()
        for scope in scopes:
            stmt = f"CREATE TABLE IF NOT EXISTS {scope.table} ({gen_column_defines(scope)})"
            cursor.execute(stmt)
        self.conn.commit()

    def _init_scopes(self, scopes: List[SqliteScopeConfig], all_table_as_scope: bool):
        # if some scopes have given, add to the scope mapper
        # if table not exists, create it
        if scopes:
            for config in scopes:
                self._add_scope(config)
            self._create_table_if_not_exists(*scopes)

        # find tables in database, if table not configured, add to the scope mapper
        if all_table_as_scope:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            # table should not bound to a scope, and should not be same as a scope
            tables = [x[0] for x in cursor.fetchall() if x[0] not in self._tables and x[0] not in self._scopes]
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = list(cursor.fetchall())
                assert len(columns[0]) == 6
                uniq_id = [x[1] for x in columns if x[5] > 0]
                if len(uniq_id) != 1:
                    raise ValueError("there should exactly one primary key column")
                self._add_scope(SqliteScopeConfig(
                    scope=table,    # use table name as scope
                    table=table,
                    uniq_id=uniq_id[0],     # primary key column as id
                    columns=[x[1] for x in columns],    # second is column name
                ))

    def _get_scope_config(self, scope: str) -> SqliteScopeConfig:
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

    @staticmethod
    def _serialize_column_value(v: Any) -> str:
        if v is None:
            return "null"
        return json.dumps(v)

    def exists(self, key: str, scope: str = None, **kwargs) -> bool:
        config = self._get_scope_config(scope)
        self.cursor.execute(f"SELECT {config.uniq_id} FROM {scope}"
                            f" WHERE {config.uniq_id} = {self._serialize_column_value(key)} LIMIT 1")
        return self.cursor.fetchone() is not None

    def fetch(self, key: str, default: Any = None, scope: Any = None, **kwargs) -> Optional[Dict[str, Any]]:
        config = self._get_scope_config(scope)
        stmt = (f"SELECT {','.join(config.columns)} FROM {scope}"
                f" WHERE {config.uniq_id} = {self._serialize_column_value(key)} LIMIT 1")
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        row = cursor.fetchone()
        if row is None:
            return default
        return dict(zip(config.columns, row))

    def set(self, key: str, value: Dict[str, Any], scope: Any = None, **kwargs) -> bool:
        config = self._get_scope_config(scope)
        if config.uniq_id in value:
            if key != value[config.uniq_id]:
                raise ValueError(f"key {key} is different from value {value[config.uniq_id]}")
            row = {name: value[name] for name in config.columns_with_id if name in value}
        else:
            row = {config.uniq_id: key}
            row.update((name, value[name]) for name in config.columns if name in value)
        stmt = (f"INSERT INTO {scope}({','.join(row.keys())})"
                f" VALUES ({','.join(map(self._serialize_column_value, row.values()))})"
                f" ON CONFLICT({config.uniq_id}) DO UPDATE"
                f" SET {','.join(f'{k}={self._serialize_column_value(v)}' for k, v in row.items())}")
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        self.conn.commit()
        return True

    def pop(self, key: str, scope: str = None, **kwargs) -> None:
        config = self._get_scope_config(scope)
        stmt = f"DELETE FROM {scope} WHERE {config.uniq_id} = {self._serialize_column_value(key)}"
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        self.conn.commit()

    def scopes(self) -> Iterable[str]:
        return self._scopes.keys()

    def keys(self, scope: str = None) -> Iterable[str]:
        config = self._get_scope_config(scope)
        stmt = f"SELECT {config.uniq_id} FROM {scope}"
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        for row in cursor.fetchall():
            yield row[0]
