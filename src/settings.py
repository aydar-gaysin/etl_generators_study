import utils.psycopg2 as psycopg2_utils

POSTGRESQL_DATABASE_SETTINGS: psycopg2_utils.DbSettings = {
    "dbname": "etl-generators",
    "user": "app",
    "password": "123qwe",
    "host": "127.0.0.1",
    "port": 6437,
}
