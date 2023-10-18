# -*- coding: utf-8 -*-

from .version import __version__
from .core import (
    Cache, NoOpCache, MemoryCache,
    LocalFileCache, SimpleLocalFileCache,
    BinaryFileCache, SimpleBinaryFileCache,
    TextFileCache, SimpleTextFileCache,
    MiddleCache,
)
from .db import *
try:
    from .redis import RedisCache
except ImportError:
    pass
try:
    from .es import ElasticSearchCache
except ImportError:
    pass
