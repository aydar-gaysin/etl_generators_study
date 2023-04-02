import collections.abc as collections_abc
import dataclasses
import itertools
import typing

import faker
import psycopg2.extras as psycopg2_extras

import settings
import utils.profilers as profiler_utils
import utils.psycopg2 as psycopg2_utils

SIZE = 50_000
CHUNK_SIZE = 500


@dataclasses.dataclass
class LoadUser:
    name: str
    description: str


def gen_fake_users() -> collections_abc.Iterator[LoadUser]:
    fake = faker.Faker()
    return (LoadUser(name=fake.name(), description=fake.text()) for _ in range(SIZE))


def create_tables(connection: psycopg2_utils.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                id serial primary key,
                name text NOT NULL,
                description text NOT NULL
            )
            """
        )
    connection.commit()


def load_data(connection: psycopg2_utils.Connection) -> None:
    loader = psycopg2_utils.Loader(connection, 500)
    users = gen_fake_users()
    loader.load_from_iterable(users, LoadUser, "users")


def drop_tables(connection: psycopg2_utils.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE users")
    connection.commit()


@dataclasses.dataclass
class ExtractUser:
    id: int
    name: str
    description: str


ExecuteType = collections_abc.Callable[[psycopg2_utils.Connection], collections_abc.Iterable[ExtractUser]]


@profiler_utils.profile
def run_through_iterable(items: collections_abc.Iterable[ExtractUser]) -> None:
    for _ in items:
        ...


@profiler_utils.profile
def fetch_all_list(connection: psycopg2_utils.Connection) -> list[ExtractUser]:
    with connection.cursor(cursor_factory=psycopg2_extras.DictCursor) as cursor:
        cursor.execute("SELECT id, name, description FROM users ORDER BY id")
        users = cursor.fetchall()
        return [ExtractUser(**user) for user in users]


@profiler_utils.profile
def fetch_all_gen_expression(connection: psycopg2_utils.Connection) -> collections_abc.Iterator[ExtractUser]:
    with connection.cursor(cursor_factory=psycopg2_extras.DictCursor) as cursor:
        cursor.execute("SELECT id, name, description FROM users ORDER BY id")
        users = cursor.fetchall()
        return (ExtractUser(**user) for user in users)


@profiler_utils.profile
def fetch_all_yield(connection: psycopg2_utils.Connection) -> collections_abc.Iterator[ExtractUser]:
    with connection.cursor(cursor_factory=psycopg2_extras.DictCursor) as cursor:
        cursor.execute("SELECT id, name, description FROM users ORDER BY id")
        users = cursor.fetchall()
        yield from (ExtractUser(**user) for user in users)


@profiler_utils.profile
def fetch_one_yield(connection: psycopg2_utils.Connection) -> collections_abc.Iterator[ExtractUser]:
    with connection.cursor(cursor_factory=psycopg2_extras.DictCursor) as cursor:
        cursor.execute("SELECT id, name, description FROM users ORDER BY id")
        while user := cursor.fetchone():
            yield ExtractUser(**user)


@profiler_utils.profile
def fetch_many_yield(connection: psycopg2_utils.Connection) -> collections_abc.Iterator[ExtractUser]:
    with connection.cursor(cursor_factory=psycopg2_extras.DictCursor) as cursor:
        cursor.execute("SELECT id, name, description FROM users ORDER BY id")
        while users_chunk := cursor.fetchmany(size=CHUNK_SIZE):
            yield from (ExtractUser(**user) for user in users_chunk)


@profiler_utils.profile
def fetch_limit_offset(connection: psycopg2_utils.Connection) -> collections_abc.Iterator[ExtractUser]:
    stmt = "SELECT id, name, description FROM users ORDER BY id LIMIT %s OFFSET %s"
    with connection.cursor(cursor_factory=psycopg2_extras.DictCursor) as cursor:
        for i in itertools.count():
            data = (CHUNK_SIZE, i * CHUNK_SIZE)
            cursor.execute(stmt, data)
            if not cursor.rowcount:
                break
            yield from (ExtractUser(**user) for user in cursor.fetchall())


@profiler_utils.profile
def fetch_last_id(connection: psycopg2_utils.Connection) -> collections_abc.Iterator[ExtractUser]:
    last_id = None
    with connection.cursor(cursor_factory=psycopg2_extras.DictCursor) as cursor:
        while True:
            stmt = "SELECT id, name, description FROM users"
            data: list[typing.Any] = []
            if last_id is not None:
                stmt += " WHERE id > %s"
                data.append(last_id)

            stmt += " ORDER BY id LIMIT %s"
            data.append(CHUNK_SIZE)

            cursor.execute(stmt, data)
            if not cursor.rowcount:
                break
            for user in cursor.fetchall():
                yield ExtractUser(**user)
            last_id = user["id"]


def run_execution(func: ExecuteType, connection: psycopg2_utils.Connection) -> None:
    iterable = func(connection)
    run_through_iterable(iterable)
    print("-" * 100)


def run() -> None:
    with psycopg2_utils.open_connection(settings.POSTGRESQL_DATABASE_SETTINGS) as connection:
        create_tables(connection)
        load_data(connection)
        try:
            run_execution(fetch_all_list, connection)
            run_execution(fetch_all_gen_expression, connection)
            run_execution(fetch_all_yield, connection)
            run_execution(fetch_one_yield, connection)
            run_execution(fetch_many_yield, connection)
            run_execution(fetch_limit_offset, connection)
            run_execution(fetch_last_id, connection)
        finally:
            connection.rollback()
            drop_tables(connection)


if __name__ == "__main__":
    run()
