import collections.abc as collections_abc
import dataclasses
import itertools
import typing

import faker
import more_itertools

import settings
import utils.profilers as profiler_utils
import utils.psycopg2 as psycopg2_utils

SIZE = 10_000
CHUNK_SIZE = 500


def create_tables(connection: psycopg2_utils.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                id serial,
                name text NOT NULL,
                description text NOT NULL
            )
            """
        )
    connection.commit()


def truncate_tables(connection: psycopg2_utils.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE users")
    connection.commit()


def drop_tables(connection: psycopg2_utils.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE users")
    connection.commit()


@dataclasses.dataclass
class User:
    name: str
    description: str


def gen_fake_users() -> collections_abc.Iterator[User]:
    fake = faker.Faker()
    return (User(name=fake.name(), description=fake.text()) for _ in range(SIZE))


ExecuteType = collections_abc.Callable[[psycopg2_utils.Connection, typing.Iterator[User]], None]


@profiler_utils.profile
def execute_single(connection: psycopg2_utils.Connection, users: collections_abc.Iterator[User]) -> None:
    with connection.cursor() as cursor:
        for user in users:
            stmt = "INSERT INTO users (name, description) VALUES (%s, %s)"
            data = (user.name, user.description)
            cursor.execute(stmt, data)
    connection.commit()


@profiler_utils.profile
def executemany(connection: psycopg2_utils.Connection, users: collections_abc.Iterator[User]) -> None:
    with connection.cursor() as cursor:
        stmt = "INSERT INTO users (name, description) VALUES (%s, %s)"
        data = ((user.name, user.description) for user in users)
        cursor.executemany(stmt, data)
    connection.commit()


@profiler_utils.profile
def execute_single_query(connection: psycopg2_utils.Connection, users: collections_abc.Iterator[User]) -> None:
    data = list(itertools.chain.from_iterable((user.name, user.description) for user in users))
    stmt = f"INSERT INTO users (name, description) VALUES {','.join('(%s, %s)' for _ in range(len(data)//2))}"
    with connection.cursor() as cursor:
        cursor.execute(stmt, data)
    connection.commit()


@profiler_utils.profile
def execute_chunks(connection: psycopg2_utils.Connection, users: collections_abc.Iterator[User]) -> None:
    stmt = "INSERT INTO users (name, description) VALUES (%s, %s)"
    with connection.cursor() as cursor:
        for user_chunk in more_itertools.ichunked(users, CHUNK_SIZE):
            for user in user_chunk:
                data = (user.name, user.description)
                cursor.execute(stmt, data)
            connection.commit()


@profiler_utils.profile
def executemany_chunks(connection: psycopg2_utils.Connection, users: collections_abc.Iterator[User]) -> None:
    stmt = "INSERT INTO users (name, description) VALUES (%s, %s)"
    with connection.cursor() as cursor:
        for user_chunk in more_itertools.ichunked(users, CHUNK_SIZE):
            data = ((user.name, user.description) for user in user_chunk)
            cursor.executemany(stmt, data)
            connection.commit()


@profiler_utils.profile
def execute_single_query_chunks(connection: psycopg2_utils.Connection, users: collections_abc.Iterator[User]) -> None:
    with connection.cursor() as cursor:
        for user_chunk in more_itertools.ichunked(users, CHUNK_SIZE):
            data = list(itertools.chain.from_iterable((user.name, user.description) for user in user_chunk))
            stmt = f"INSERT INTO users (name, description) VALUES {','.join('(%s, %s)' for _ in range(len(data)//2))}"
            cursor.execute(stmt, data)
            connection.commit()


def run_execution(func: ExecuteType, connection: psycopg2_utils.Connection) -> None:
    users = gen_fake_users()
    func(connection, users)
    truncate_tables(connection)
    print("-" * 100)


def run() -> None:
    with psycopg2_utils.open_connection(settings.POSTGRESQL_DATABASE_SETTINGS) as connection:
        create_tables(connection)
        try:
            run_execution(execute_single, connection)
            run_execution(executemany, connection)
            run_execution(execute_single_query, connection)
            run_execution(execute_chunks, connection)
            run_execution(executemany_chunks, connection)
            run_execution(execute_single_query_chunks, connection)
        finally:
            drop_tables(connection)


if __name__ == "__main__":
    run()
