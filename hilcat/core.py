# -*- coding: utf-8 -*-

"""
Core code for the high level abstract cache.
"""

from typing import (
    Any, Dict,
    Callable, Iterable,
    Hashable,
)
from abc import (
    ABC, abstractmethod,
)
import os
import pathlib

class Storage(ABC):
    """
    Base storage class, all api is equal to the cache expect method load() and save().

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

    def get(self, key: Any, func: Callable[[], Any] = None, scope: Any = None, **kwargs) -> Any:
        """
        If func is None, it's equal to method `fetch()`.
        If func not None,
            get value from cache if key exists;
            else run the func and save value to the cache.
        """
        if func is None or self.exists(key, scope=scope):
            return self.fetch(key, scope=scope)
        value = func()
        self.set(key, value, scope=scope)
        return value

    @abstractmethod
    def pop(self, key: Any, scope: Any = None, **kwargs) -> Any:
        """
        Delete value for the given key.
        :return:    Value in the cache if arg return_value is `True`.
        """

    def load(self, scopes: Iterable[Any] = None, **kwargs):
        """
        Load scope data from persistence storage.
        """

    def save(self, scopes: Iterable[Any] = None, **kwargs):
        """
        Save scope data to persistence storage.

        If cache is just a temp storage, this method should be implemented.
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

class Cache(Storage, ABC):
    """
    An abstract cache interface.
    """

    def load(self, scopes: Iterable[Any] = None, **kwargs):
        """
        Load scope data from persistence storage.
        """

    def save(self, scopes: Iterable[Any] = None, **kwargs):
        """
        Save scope data to persistence storage.

        If cache is just a temp storage, this method should be implemented.
        """

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

class MemoryCache(Cache, ABC):
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
    def get_filepath(self, key: Any, scope: Any = None) -> str:
        """
        Get filepath for the given key to save value.
        """

    def exists(self, key: Any, scope: Any = None, **kwargs) -> bool:
        filepath = self.get_filepath(key, scope=scope)
        return os.path.exists(filepath)

    @abstractmethod
    def read_file(self, filepath: str) -> Any:
        """
        Read file content.
        """

    def fetch(self, key: Any, default: Any = None, scope: Any = None, **kwargs) -> Any:
        """
        If file exists, read file content; else, return default.
        """
        filepath = self.get_filepath(key, scope=scope)
        if os.path.exists(filepath):
            return self.read_file(filepath)
        return default

    def write_file0(self, filepath: str, content: Any):
        """
        The actual write file method.
        """
        # Someone may overwrite write_file() and this method would not be called.
        # If it's not necessary, there is no impact for raising and error.
        raise NotImplementedError()

    def write_file(self, filepath: str, content: Any):
        """
        Write content to file.
        """
        # create parent dir first
        os.makedirs(os.path.abspath(os.path.dirname(filepath)), exist_ok=True)
        self.write_file0(filepath, content)

    def set(self, key: Any, value: Any, scope: Any = None, **kwargs) -> bool:
        filepath = self.get_filepath(key, scope=scope)
        self.write_file(filepath, value)
        return True

    def update(self, key: Any, value: Any, scope: Any = None, return_old=False, **kwargs) -> Any:
        filepath = self.get_filepath(key, scope=scope)
        result = None
        if return_old and os.path.exists(filepath):
            result = self.read_file(filepath)
        self.write_file(filepath, value)
        return result

    def pop(self, key: Any, scope: Any = None, return_old=False, **kwargs) -> Any:
        filepath = self.get_filepath(key, scope=scope)
        result = None
        if os.path.exists(filepath):
            if return_old:
                result = self.read_file(filepath)
            os.remove(filepath)
        return result

class BinaryFileCache(LocalFileCache, ABC):
    """
    Each value stored in a binary file.
    """
    def read_file(self, filepath: str) -> Any:
        with open(filepath, 'rb') as f:
            return f.read()

    def write_file0(self, filepath: str, content: Any):
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

    def read_file(self, filepath: str) -> str:
        with open(filepath, encoding=self.encoding) as f:
            return f.read()

    def write_file0(self, filepath: str, content: str):
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

    def get_filepath(self, key: str, scope: str = None) -> str:
        return os.path.join(self.root_dir, scope or '', key + self.suf)

    def keys(self, scope: Any = None) -> Iterable[str]:
        # find all files under the directory
        root = pathlib.Path(self.root_dir)
        for f in root.rglob("*" + self.suf):
            if f.is_file():
                yield f.relative_to(root).as_posix()[:-len(self.suf)]

class SimpleBinaryFileCache(SimpleLocalFileCache, BinaryFileCache):
    def __init__(self, root_dir: str, suf: str = ''):
        SimpleLocalFileCache.__init__(self, root_dir, suf)
        BinaryFileCache.__init__(self)

class SimpleTextFileCache(SimpleLocalFileCache, TextFileCache):
    def __init__(self, root_dir: str, suf: str = '', encoding='utf-8'):
        SimpleLocalFileCache.__init__(self, root_dir, suf)
        TextFileCache.__init__(self, encoding)

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

    def save(self, scopes: Iterable[Any] = None, **kwargs):
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

