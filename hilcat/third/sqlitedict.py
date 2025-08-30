from typing import Any, Iterable, Dict, Union, Sequence
import os
import re
from sqlitedict import SqliteDict
from hilcat.core import RegistrableCache

def get_from_dict_or_default(d: dict, keys: list, default=None):
    for k in keys:
        if k in d:
            return d[k]
    return default

class SqliteDictCache(RegistrableCache):
    """
    Use a sqlite database as backend.
    """

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'SqliteDictCache':
        assert re.match(r'\w+:///.+', uri), uri
        schema, database = uri.split(':///')
        assert schema.lower() == 'sqlite', schema
        return cls(db_file=database, **kwargs)

    def __init__(
        self, db_file: Union[str, os.PathLike], autocommit: bool = True,
        scopes: Union[str, Dict[str, Union[str, Dict[str, Any]]], Sequence[Union[str, Dict[str, Any]]]] = None,
    ):
        os.makedirs(os.path.dirname(db_file), exist_ok=True)    # ensure db_file directory exists
        self._db_file = db_file
        self.autocommit = autocommit
        self._cache = {}
        if scopes is None:
            pass
        elif isinstance(scopes, str):
            self._init_scope(scopes)
        elif isinstance(scopes, dict):
            for scope, config in scopes.items():
                if isinstance(config, str):
                    self._init_scope(scope, table_name=config)
                elif isinstance(config, dict):
                    self._init_scope(scope, **config)
                else:
                    raise ValueError(f"Invalid scope config type: {type(config)}")
        else:
            for config in scopes:
                if isinstance(config, str):
                    self._init_scope(config)
                elif isinstance(config, dict):
                    self._init_scope(**config)
                else:
                    raise ValueError(f"Invalid scope config type: {type(config)}")

    def _init_scope(self, scope: str, **kwargs):
        table_name = get_from_dict_or_default(kwargs, ['table_name', 'tablename', 'table'], default=scope)
        autocommit = get_from_dict_or_default(kwargs, ['autocommit', 'auto_commit'], default=self.autocommit)
        self._cache[scope] = SqliteDict(self._db_file, tablename=table_name, autocommit=autocommit)

    def _has_scope_cache(self, scope: str) -> bool:
        return scope in self._cache

    def _get_scope_cache(self, scope: str) -> SqliteDict:
        if scope not in self._cache:
            self._init_scope(scope)
        return self._cache[scope]

    def exists(self, key: Any, scope: str = None, **kwargs) -> bool:
        if self._has_scope_cache(scope):
            d = self._get_scope_cache(scope)
            return key in d
        return False

    def fetch(self, key: Any, default: Any = None, scope: str = None, **kwargs) -> Any:
        if self._has_scope_cache(scope):
            d = self._get_scope_cache(scope)
            if key in d:
                return d[key]
        return default

    def set(self, key: Any, value: Any, scope: str = None, **kwargs) -> Any:
        d = self._get_scope_cache(scope)
        d[key] = value

    def pop(self, key: Any, scope: str = None, **kwargs) -> Any:
        if self._has_scope_cache(scope):
            d = self._get_scope_cache(scope)
            return d.pop(key)
        return None

    def scopes(self) -> Iterable[str]:
        return self._cache.keys()

    def keys(self, scope: str = None) -> Iterable[Any]:
        if self._has_scope_cache(scope):
            d = self._get_scope_cache(scope)
            return d.keys()
        return []

