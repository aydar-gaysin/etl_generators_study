import collections.abc as collections_abc
import contextlib
import sqlite3
import typing

Connection = sqlite3.Connection


class DbSettings(typing.TypedDict):
    database: str


@contextlib.contextmanager
def open_connection(settings: DbSettings) -> collections_abc.Iterator[sqlite3.Connection]:
    con = sqlite3.connect(**settings, detect_types=sqlite3.PARSE_DECLTYPES)
    try:
        yield con
    finally:
        con.close()
