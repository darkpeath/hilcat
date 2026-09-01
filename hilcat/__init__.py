# -*- coding: utf-8 -*-

from .version import __version__
from .core import (
    Cache, NoOpCache, MemoryCache,
    LocalFileCache, SimpleLocalFileCache,
    BinaryFileCache, SimpleBinaryFileCache,
    TextFileCache, SimpleTextFileCache,
    SimpleJsonFileCache,
    MiddleCache, MemoryMiddleCache,
    CacheAgent,
    register_backend,
)
from .db.relational import (
    SqlBuilder,
    SimpleSqlBuilder,
    QmarkSqlBuilder,
    NumericSqlBuilder,
    NamedSqlBuilder,
    FormatSqlBuilder,
    PyformatSqlBuilder,
    RelationalDbScopeConfig,
    RelationalDbCache,
    SingleTableConfig,
    SingleTableCache,
)

# names of optional backends are appended when the import succeeds
__all__ = [
    '__version__',
    'Cache', 'NoOpCache', 'MemoryCache',
    'LocalFileCache', 'SimpleLocalFileCache',
    'BinaryFileCache', 'SimpleBinaryFileCache',
    'TextFileCache', 'SimpleTextFileCache',
    'SimpleJsonFileCache',
    'MiddleCache', 'MemoryMiddleCache',
    'CacheAgent',
    'register_backend',
    'SqlBuilder', 'SimpleSqlBuilder',
    'QmarkSqlBuilder', 'NumericSqlBuilder', 'NamedSqlBuilder',
    'FormatSqlBuilder', 'PyformatSqlBuilder',
    'RelationalDbScopeConfig', 'RelationalDbCache',
    'SingleTableConfig', 'SingleTableCache',
]

try:
    from .db.redis import RedisCache
except ImportError:
    pass
else:
    register_backend('redis', RedisCache)
    __all__.append('RedisCache')

try:
    from .db.es import ElasticSearchCache
except ImportError:
    pass
else:
    register_backend('es', ElasticSearchCache)
    __all__.append('ElasticSearchCache')

try:
    from .db.sqlite import (
        SqliteSqlBuilder,
        SqliteScopeConfig,
        SqliteCache,
        SqliteSingleTableCache,
    )
except ImportError:
    pass
else:
    register_backend('sqlite', SqliteCache)
    __all__ += ['SqliteSqlBuilder', 'SqliteScopeConfig', 'SqliteCache', 'SqliteSingleTableCache']

try:
    from .db.postgresql import (
        PostgresqlBuilder,
        PostgresqlScopeConfig,
        PostgresqlCache,
        PostgresqlSingleTableCache,
    )
except ImportError:
    # psycopg maybe not installed
    pass
else:
    register_backend('postgresql', PostgresqlCache)
    __all__ += ['PostgresqlBuilder', 'PostgresqlScopeConfig', 'PostgresqlCache', 'PostgresqlSingleTableCache']

try:
    from .db.mysql import (
        MysqlSqlBuilder,
        MysqlScopeConfig,
        MysqlCache,
        MysqlSingleTableCache,
    )
except ImportError:
    # pymysql or mysql.connector should be installed
    pass
else:
    register_backend('mysql', MysqlCache)
    __all__ += ['MysqlSqlBuilder', 'MysqlScopeConfig', 'MysqlCache', 'MysqlSingleTableCache']

try:
    # sqlitedict should be installed
    from .third.sqlitedict import SqliteDictCache
except ImportError:
    pass
else:
    __all__.append('SqliteDictCache')
