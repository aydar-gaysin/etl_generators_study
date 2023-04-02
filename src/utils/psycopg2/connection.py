import collections.abc as collections_abc
import contextlib
import typing

import psycopg2
import psycopg2.extensions as psycopg2_extensions

Connection = psycopg2_extensions.connection


class DbSettings(typing.TypedDict):
    dbname: str
    user: str
    password: str
    host: str
    port: int


@contextlib.contextmanager
def open_connection(settings: DbSettings) -> collections_abc.Iterator[psycopg2_extensions.connection]:
    con = psycopg2.connect(**settings)
    try:
        yield con
    finally:
        con.close()
