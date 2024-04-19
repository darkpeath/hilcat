# -*- coding: utf-8 -*-

from .version import __version__
from .core import (
    Cache, NoOpCache, MemoryCache,
    LocalFileCache, SimpleLocalFileCache,
    BinaryFileCache, SimpleBinaryFileCache,
    TextFileCache, SimpleTextFileCache,
    MiddleCache, MemoryMiddleCache,
    CacheAgent,
    register_backend,
)

try:
    from .redis import RedisCache
except ImportError:
    pass
else:
    register_backend('redis', RedisCache)

try:
    from .es import ElasticSearchCache
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
)

try:
    from .db.sqlite import (
        SqliteSqlBuilder,
        SqliteScopeConfig,
        SqliteCache,
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
    )
except ImportError:
    # pymysql or mysql.connector should be installed
    pass
else:
    register_backend('mysql', MysqlCache)

