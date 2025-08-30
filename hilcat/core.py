# -*- coding: utf-8 -*-

"""
Core code for the high level abstract cache.
"""

from typing import (
    Any, Dict, List,
    Callable, Iterable,
    Hashable, Optional,
    Type, Union, Sequence,
)
from abc import (
    ABC, abstractmethod,
)
import os
import sys
import json
import inspect
import pathlib
import warnings
import builtins
import functools
import urllib.parse

def _create_fn(name: str, args: List[str], body: List[str], *,
               _globals: Dict[str, Any] = None,
               _locals: Dict[str, Any] = None,
               return_type: Optional[Type] = None):
    _globals = _globals or {}
    # Note that we mutate locals when exec() is called.  Caller
    # beware!  The only callers are internal to this module, so no
    # worries about external callers.
    if _locals is None:
        _locals = {}
    if 'BUILTINS' not in _locals:
        _locals['BUILTINS'] = builtins
    return_annotation = ''
    if return_type is not None:
        _locals['_return_type'] = return_type
        return_annotation = '->_return_type'
    args = ','.join(args)
    body = '\n'.join(f'  {b}' for b in body)

    # Compute the text of the entire function.
    txt = f' def {name}({args}){return_annotation}:\n{body}'

    local_vars = ', '.join(_locals.keys())
    txt = f"def __create_fn__({local_vars}):\n{txt}\n return {name}"

    ns = {}
    exec(txt, _globals, ns)
    return ns['__create_fn__'](**_locals)

class Storage(ABC):
    """
    Base storage class, all api is equal to the cache except method load() and save().

    Any key is under a scope, but scope can be always same value or None in some implements.

    The key is recommended to use an uniq id as key, but actually it can be anything.
    The scope is recommended to use a string.
    """

    def close(self):
        """
        For some remote server, close the connection.

        For some local file cache, close and save files.
        """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getitem__(self, item):
        # just fetch value from default scope
        return self.fetch(item)

    def __setitem__(self, key, value):
        # just set value as to default scope
        self.set(key, value)

    @abstractmethod
    def exists(self, key: Any, scope: Any = None, **kwargs) -> bool:
        """
        Check if given key exists in the cache.
        """

    @abstractmethod
    def fetch(self, key: Any, default: Any = None, scope: Any = None, **kwargs) -> Any:
        """
        Fetch value of the given key.
        Maybe raise an error or return a default value when key not exists.
        """

    @abstractmethod
    def set(self, key: Any, value: Any, scope: Any = None, **kwargs) -> Any:
        """
        Set value for the given key.
        :return:    Can be old value, merged value, a bool or anything depends on the implement.
        """

    def update(self, key: Any, value: Any, scope: Any = None, **kwargs) -> Any:
        """
        Update value for the given key.
        """
        # default is same as set
        return self.set(key, value, scope=scope, **kwargs)

    def get(self, key: Any, func: Callable = None, func_args: Sequence[Any] = None,
            func_kwargs: Dict[str, Any] = None, scope: Any = None, **kwargs) -> Any:
        """
        If func is None, it's equal to method `fetch()`.
        If func not None,
            get value from cache if key exists;
            else run the func and save value to the cache.
        """
        if func is None or self.exists(key, scope=scope):
            return self.fetch(key, scope=scope)
        func_args = func_args or []
        func_kwargs = func_kwargs or {}
        value = func(*func_args, **func_kwargs)
        self.set(key, value, scope=scope)
        return value

    @abstractmethod
    def pop(self, key: Any, scope: Any = None, **kwargs) -> Any:
        """
        Delete value for the given key.
        :return:    Value in the cache if arg return_value is `True`.
        """

    def scopes(self) -> Iterable[Any]:
        """
        Get all scopes in the cache.
        """
        raise NotImplementedError("It's not allowed to get all scopes.")

    def keys(self, scope: Any = None) -> Iterable[Any]:
        """
        Get all keys in the cache.
        """
        raise NotImplementedError("It's not allowed to get all keys.")

    def __call__(self, f=None, scope=None, ignore_first_arg=False, make_key_func=None, **kwargs):
        """
        Decorate function with a cache.
        To cache result of the function.
        :param f:           function to be decorated
        :param scope:       scope for cache to get and set
        :param ignore_first_arg:    whether to ignore function first arg when gen cache key;
                                    if function is a class method, this can be set as `True`
        :param make_key_func:   a function to gen key from arg values
        """

        def wrap(_f):
            def get_var(var1: str, var2: str):
                return var1 if var1 != _f.__name__ else var2

            sig = inspect.signature(_f)
            parameters = list(sig.parameters.keys())
            if ignore_first_arg:
                parameters = parameters[1:]
            if len(parameters) == 0:
                raise ValueError("Function consume no arg.")

            has_kwargs = list(sig.parameters.values())[-1].kind.value == 5
            make_key_var = get_var("_make_key", "__make_key")
            make_key = make_key_func
            if not make_key:
                if len(parameters) == 1:
                    if has_kwargs:
                        def make_key(*args, **kwargs):
                            bind = sig.bind(*args, **kwargs)
                            return tuple(sorted(bind.arguments[parameters[0]].items()))
                    else:
                        def make_key(*args, **kwargs):
                            bind = sig.bind(*args, **kwargs)
                            return bind.arguments[parameters[0]]
                else:
                    if has_kwargs:
                        def make_key(*args, **kwargs):
                            bind = sig.bind(*args, **kwargs)
                            return (tuple((x, bind.arguments[x]) for x in parameters[:-1]) +
                                    tuple(sorted(bind.arguments[parameters[-1]].items())))
                    else:
                        def make_key(*args, **kwargs):
                            bind = sig.bind(*args, **kwargs)
                            return tuple(map(bind.arguments.get, parameters))

            _globals = sys.modules[_f.__module__].__dict__
            func_var = get_var("_func", "__func")
            score_var = get_var("_scope", "_scope")
            cache_var = get_var("_cache", "__cache")
            _locals = {
                "BUILTINS": builtins,
                func_var: _f,
                score_var: scope,
                cache_var: self,
                make_key_var: make_key,
            }
            args = ["*args", "**kwargs"]
            body_lines = [
                f"return {cache_var}.get({make_key_var}(*args, **kwargs),"
                f" lambda: {func_var}(*args, **kwargs), scope={score_var})"
            ]
            return_type = _f.__annotations__.get('return')
            func = _create_fn(_f.__name__, args, body_lines,
                              _globals=_f.__globals__,
                              _locals=_locals,
                              return_type=return_type)
            func = functools.update_wrapper(func, _f)
            return func

        # See if we're being called as @Cache() or @Cache()().
        if f is None:
            # We're called with 2 pair of parens.
            return wrap

        # We're called with 1 pair of parens.
        return wrap(f)

class Cache(Storage, ABC):
    """
    An abstract cache interface.
    """

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'Cache':
        """
        Create a cache base on given backend.
        Auto-detect backend engine.
        Subclass should override this method.
        """
        r = urllib.parse.urlsplit(uri)
        backend = BACKENDS.get(r.scheme, DEFAULT_BACKEND)
        if backend is None:
            if r.scheme:
                raise ValueError(f"schema not given: {uri}")
            raise ValueError(f"Unsupported backend: {r.scheme}")
        if isinstance(backend, Cache):
            # engine is a cache instance, return it directly
            return backend
        if inspect.isclass(backend) and issubclass(backend, Cache):
            # engine is a cache subclass, create it
            return backend.from_uri(uri, **kwargs)
        if callable(backend):
            # engine is a function, call it
            # should inspect function signature and pass different args ?
            try:
                return backend(uri, **kwargs)
            except TypeError:
                return backend(uri, kwargs)
        raise RuntimeError(f"Unexpected engine type: {type(backend)}")

    def load(self, scopes: Iterable[Any] = None, **kwargs):
        """
        Load scope data from persistence storage.
        """

    def backup(self, scopes: Iterable[Any] = None, **kwargs):
        """
        Save scope data to persistence storage.

        If cache is just a temp storage, this method should be implemented.
        """

    def save(self, scopes: Iterable[Any] = None, **kwargs):
        # backup has no ambiguity, save is going to be deprecated
        warnings.warn("use backup() instead", DeprecationWarning)
        self.backup(scopes, **kwargs)

# schema -> cache builder; to build a cache from uri
_BACKEND_TYPE = Union[Cache, Type[Cache], Callable[[str, Dict[str, Any]], Cache]]
BACKENDS: Dict[str, _BACKEND_TYPE] = {}
DEFAULT_BACKEND: Optional[_BACKEND_TYPE] = None

def register_backend(schema: str, backend: _BACKEND_TYPE):
    """
    Register a backend, then we can create the cache by `Cache.from_uri()`.
    """
    if schema in BACKENDS:
        warnings.warn(f"Backend {schema} already defined, it will be overwritten.")
    BACKENDS[schema] = backend

class RegistrableCache(Cache, ABC):
    """
    Subclasses are forced to implement the `from_uri` method.
    """
    @classmethod
    @abstractmethod
    def from_uri(cls, uri: str, **kwargs) -> Cache:
        pass

class NoOpCache(Cache):
    """
    A cache placeholder, used when all values are calculated currently but may need a cache some day.
    """
    def exists(self, key: Any, scope: Any = None, **kwargs) -> bool:
        return False

    def fetch(self, key: Any, default: Any = None, scope: Any = None, **kwargs) -> Any:
        return None

    def set(self, key: Any, value: Any, scope: Any = None, **kwargs) -> None:
        pass

    def update(self, key: Any, value: Any, scope: Any = None, **kwargs) -> Any:
        return None

    def pop(self, key: Any, scope: Any = None, **kwargs) -> Any:
        return None

class MemoryCache(Cache):
    """
    Use a dict as backend.
    """
    def __init__(self) -> None:
        self.data = {}

    def exists(self, key: Hashable, scope: Hashable = None, **kwargs) -> bool:
        return scope in self.data and key in self.data[scope]

    def fetch(self, key: Hashable, default: Any = None, scope: Hashable = None, **kwargs) -> Any:
        return self.data.get(scope, {}).get(key, default)

    def set(self, key: Hashable, value: Any, scope: Hashable = None, **kwargs) -> bool:
        if scope not in self.data:
            self.data[scope] = {}
        self.data[scope][key] = value
        return True

    def update(self, key: Hashable, value: Any, scope: Hashable = None, **kwargs) -> Any:
        old = None
        if self.exists(key, scope=scope):
            old = self.fetch(key, scope=scope)
        self.set(key, value, scope=scope)
        return old

    def pop(self, key: Any, scope: Any = None, **kwargs) -> Any:
        if self.exists(key, scope=scope):
            return self.data[scope].pop(key)
        return None

    def scopes(self) -> Iterable[Hashable]:
        return self.data.keys()

    def keys(self, scope: Any = None) -> Iterable[Hashable]:
        return self.data.get(scope, {}).keys()

class LocalFileCache(Cache):
    """
    Each key corresponds to a file.
    """
    @abstractmethod
    def _get_filepath(self, key: Any, scope: Any = None) -> str:
        """
        Get filepath for the given key to save value.
        """

    def exists(self, key: Any, scope: Any = None, **kwargs) -> bool:
        filepath = self._get_filepath(key, scope=scope)
        return os.path.exists(filepath)

    @abstractmethod
    def _read_file(self, filepath: str) -> Any:
        """
        Read file content.
        """

    def fetch(self, key: Any, default: Any = None, scope: Any = None, **kwargs) -> Any:
        """
        If file exists, read file content; else, return default.
        """
        filepath = self._get_filepath(key, scope=scope)
        if os.path.exists(filepath):
            return self._read_file(filepath)
        return default

    def _write_file0(self, filepath: str, content: Any):
        """
        The actual write file method.
        """
        # Someone may overwrite _write_file() and this method would not be called.
        # If it's not necessary, there is no impact for raising an error.
        raise NotImplementedError()

    def _write_file(self, filepath: str, content: Any):
        """
        Write content to file.
        """
        # create parent dir first
        os.makedirs(os.path.abspath(os.path.dirname(filepath)), exist_ok=True)
        self._write_file0(filepath, content)

    def set(self, key: Any, value: Any, scope: Any = None, **kwargs) -> bool:
        filepath = self._get_filepath(key, scope=scope)
        self._write_file(filepath, value)
        return True

    def update(self, key: Any, value: Any, scope: Any = None, return_old=False, **kwargs) -> Any:
        filepath = self._get_filepath(key, scope=scope)
        result = None
        if return_old and os.path.exists(filepath):
            result = self._read_file(filepath)
        self._write_file(filepath, value)
        return result

    def pop(self, key: Any, scope: Any = None, return_old=False, **kwargs) -> Any:
        filepath = self._get_filepath(key, scope=scope)
        result = None
        if os.path.exists(filepath):
            if return_old:
                result = self._read_file(filepath)
            os.remove(filepath)
        return result

class BinaryFileCache(LocalFileCache, ABC):
    """
    Each value stored in a binary file.
    """
    def _read_file(self, filepath: str) -> Any:
        with open(filepath, 'rb') as f:
            return f.read()

    def _write_file0(self, filepath: str, content: Any):
        with open(filepath, 'wb') as f:
            f.write(content)

class TextFileCache(LocalFileCache, ABC):
    """
    Each value stored in a text file.
    """
    def __init__(self, encoding='utf-8'):
        """
        :param encoding:        file encoding
        """
        self.encoding = encoding

    def _read_file(self, filepath: str) -> str:
        with open(filepath, encoding=self.encoding) as f:
            return f.read()

    def _write_file0(self, filepath: str, content: str):
        with open(filepath, 'w', encoding=self.encoding) as f:
            f.write(content)

class SimpleLocalFileCache(LocalFileCache, ABC):
    """
    All files saved in a directory, and key to be relative path without suffix.
    """
    def __init__(self, root_dir: str, suf: str = ''):
        """
        :param root_dir:    root dir for files
        :param suf:         commonly should start with '.'
        """
        self.root_dir = root_dir
        self.suf = suf

    def _get_filepath(self, key: str, scope: str = None) -> str:
        return os.path.join(self.root_dir, scope or '', key + self.suf)

    def keys(self, scope: str = None) -> Iterable[str]:
        # find all files under the directory
        root = pathlib.Path(self.root_dir)
        if scope:
            root = root.joinpath(scope)
        for f in root.rglob("*" + self.suf):
            if f.is_file():
                key = f.relative_to(root).as_posix()
                if self.suf:
                    key = key[:-len(self.suf)]
                yield key

class SimpleBinaryFileCache(SimpleLocalFileCache, BinaryFileCache):
    def __init__(self, root_dir: str, suf: str = ''):
        SimpleLocalFileCache.__init__(self, root_dir, suf)
        BinaryFileCache.__init__(self)

class SimpleTextFileCache(SimpleLocalFileCache, TextFileCache):
    def __init__(self, root_dir: str, suf: str = '', encoding='utf-8'):
        SimpleLocalFileCache.__init__(self, root_dir, suf)
        TextFileCache.__init__(self, encoding)

class SimpleJsonFileCache(SimpleTextFileCache):
    def __init__(self, root_dir: str, suf: str = '.json', encoding='utf-8'):
        super().__init__(root_dir, suf, encoding)

    def _read_file(self, filepath: str) -> Union[dict, list]:
        with open(filepath, encoding=self.encoding) as f:
            return json.load(f)

    def _write_file0(self, filepath: str, content: Union[dict, list]):
        with open(filepath, 'w', encoding=self.encoding) as f:
            json.dump(content, f)

class MiddleCache(Cache, ABC):
    """
    A cache which has a persistent storage.

    The storage is also a cache but ignore scope.
    The key of the storage corresponds to the scope of this cache.
    """

    def __init__(self, storage: Storage):
        self.storage = storage

    @abstractmethod
    def set_scope_values(self, data: Any, scope=None):
        """
        Set values for given scope.
        """

    def load(self, scopes: Iterable[Any] = None, **kwargs):
        if scopes is None:
            # It mab be not allowed to get all keys for the storage.
            scopes = self.storage.keys()
        for scope in scopes:
            values = self.storage.fetch(scope)
            self.set_scope_values(values, scope=scope)

    @abstractmethod
    def get_scope_values(self, scope=None) -> Any:
        """
        Get values for given scope.
        """

    def backup(self, scopes: Iterable[Any] = None, **kwargs):
        if scopes is None:
            scopes = self.scopes()
        for scope in scopes:
            values = self.get_scope_values(scope)
            self.storage.set(scope, values)

class MemoryMiddleCache(MemoryCache, MiddleCache):
    """
    Data cached in memory with a persistent storage.
    """

    def __init__(self, storage: Storage):
        MemoryCache.__init__(self)
        MiddleCache.__init__(self, storage)

    def set_scope_values(self, data: Dict[Hashable, Any], scope: Hashable = None):
        self.data[scope] = data

    def get_scope_values(self, scope: Hashable = None) -> Dict[Hashable, Any]:
        return self.data.get(scope, {})


class CacheAgent(Cache):
    """
    Use multi cache as backends when write data.
    First cache as backend when read data.
    """

    def __init__(self, backend: Cache, *extra_writers: Cache):
        self.backend = backend
        self.extra_writers = extra_writers

    def exists(self, key: Any, scope: Any = None, **kwargs) -> bool:
        return self.backend.exists(key, scope=scope, **kwargs)

    def fetch(self, key: Any, default: Any = None, scope: Any = None, **kwargs) -> Any:
        return self.backend.fetch(key, default=default, scope=scope, **kwargs)

    def set(self, key: Any, value: Any, scope: Any = None, **kwargs) -> Any:
        for backend in self.extra_writers:
            backend.set(key, value, scope=scope, **kwargs)
        return self.backend.set(key, value, scope=scope, **kwargs)

    def update(self, key: Any, value: Any, scope: Any = None, **kwargs) -> Any:
        for backend in self.extra_writers:
            backend.update(key, value, scope=scope, **kwargs)
        return self.backend.update(key, value, scope=scope, **kwargs)

    def pop(self, key: Any, scope: Any = None, **kwargs) -> Any:
        for backend in self.extra_writers:
            backend.pop(key, scope=scope, **kwargs)
        return self.backend.pop(key, scope=scope, **kwargs)

    def scopes(self) -> Iterable[Any]:
        return self.backend.scopes()

    def keys(self, scope: Any = None) -> Iterable[Any]:
        return self.backend.keys(scope=scope)

