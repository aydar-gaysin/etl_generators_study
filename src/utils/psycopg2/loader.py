import collections.abc as collections_abc
import dataclasses
import typing

import more_itertools

from .connection import Connection

T = typing.TypeVar("T")


class Loader:
    def __init__(self, connection: Connection, chunk_size: int):
        self.connection = connection
        self.chunk_size = chunk_size

    def load_from_iterable(
        self,
        items: collections_abc.Iterable[T],
        dataclass: typing.Type[T],
        table_name: str,
    ) -> None:
        if not dataclasses.is_dataclass(dataclass):
            raise ValueError

        field_names = [field.name for field in dataclasses.fields(dataclass)]
        stmt = f"INSERT INTO {table_name} ({','.join(field_names)}) VALUES ({', '.join('%s' for _ in field_names)})"
        with self.connection.cursor() as cursor:
            for chunk in more_itertools.ichunked(items, self.chunk_size):
                data = [dataclasses.astuple(item) for item in chunk]
                cursor.executemany(stmt, data)
                self.connection.commit()
