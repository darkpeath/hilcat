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

try:
    from .db.redis import RedisCache
except ImportError:
    pass
else:
    register_backend('redis', RedisCache)

try:
    from .db.es import ElasticSearchCache
except ImportError:
    pass
else:
    register_backend('es', ElasticSearchCache)

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

try:
    # sqlitedict should be installed
    from .third.sqlitedict import SqliteDictCache
except ImportError:
    pass
