from os import getenv

from pagai.services.database_explorer import MSSQL, ORACLE, POSTGRES


DATABASES = {
    MSSQL: {
        'host': getenv('TEST_MSSQL_HOST'),
        'port': int(getenv('TEST_MSSQL_PORT')),
        'database': getenv('TEST_MSSQL_DB'),
        'login': getenv('TEST_MSSQL_LOGIN'),
        'password': getenv('TEST_MSSQL_PASSWORD'),
    },
    ORACLE: {
        'host': getenv('TEST_ORACLE_HOST'),
        'port': int(getenv('TEST_ORACLE_PORT', 1531)),
        'database': getenv('TEST_ORACLE_DB'),
        'login': getenv('TEST_ORACLE_LOGIN'),
        'password': getenv('TEST_ORACLE_PASSWORD'),
    },
    POSTGRES: {
        'host': getenv('TEST_POSTGRES_HOST', 'postgres'),
        'port': int(getenv('TEST_POSTGRES_PORT', 5432)),
        'database': getenv('TEST_POSTGRES_DB', 'test'),
        'login': getenv('TEST_POSTGRES_LOGIN', 'test'),
        'password': getenv('TEST_POSTGRES_PASSWORD', 'test'),
    },
}
