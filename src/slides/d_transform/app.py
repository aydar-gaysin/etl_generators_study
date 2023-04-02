import collections.abc as collections_abc
import dataclasses
import itertools
import random
import typing

import faker
import more_itertools


def print_iterable(items: collections_abc.Iterable[typing.Any]) -> None:
    for item in items:
        print(item)


@dataclasses.dataclass
class User:
    id: int
    emails: list[str]


@dataclasses.dataclass
class UserEmail:
    user_id: int
    email: str


def gen_fake_users() -> collections_abc.Iterator[User]:
    fake = faker.Faker()
    for _id in range(0, 5):
        yield User(id=_id, emails=[fake.email() for _ in range(random.randrange(3, 5))])


def gen_fake_user_emails() -> collections_abc.Iterator[UserEmail]:
    fake = faker.Faker()
    for user_id in range(0, 5):
        for _ in range(random.randrange(3, 5)):
            yield UserEmail(user_id=user_id, email=fake.email())


def transform_user_to_user_email(users: collections_abc.Iterable[User]) -> collections_abc.Iterator[UserEmail]:
    for user in users:
        for email in user.emails:
            yield UserEmail(user_id=user.id, email=email)


def run_unpack() -> None:
    users = list(gen_fake_users())
    print_iterable(users)

    user_emails = transform_user_to_user_email(users)
    print_iterable(user_emails)


def transform_user_email_to_user(user_emails: collections_abc.Iterable[UserEmail]) -> collections_abc.Iterator[User]:
    current_user: typing.Optional[User] = None
    for user_email in user_emails:
        if current_user and current_user.id != user_email.user_id:
            yield current_user
            current_user = None
        if current_user is None:
            current_user = User(id=user_email.user_id, emails=[])
        current_user.emails.append(user_email.email)
    if current_user:
        yield current_user


def run_pack() -> None:
    user_emails = list(gen_fake_user_emails())
    print_iterable(user_emails)

    users = transform_user_email_to_user(user_emails)
    print_iterable(users)


def run_chunk() -> None:
    chunked_items = more_itertools.chunked(iterable=range(95), n=20)
    print_iterable(chunked_items)

    print("-" * 100)

    chunked_items = more_itertools.ichunked(iterable=range(95), n=20)
    print_iterable(chunked_items)


def run_chain() -> None:
    chunked = more_itertools.chunked(iterable=range(7), n=3)
    print_iterable(chunked)

    print("-" * 100)

    chunked = more_itertools.chunked(iterable=range(7), n=3)
    items = itertools.chain(*chunked)
    print_iterable(items)

    print("-" * 100)

    chunked = more_itertools.ichunked(iterable=range(7), n=3)
    items = itertools.chain(*chunked)
    print_iterable(items)

    print("-" * 100)

    chunked = more_itertools.ichunked(iterable=range(7), n=3)
    items = itertools.chain.from_iterable(chunked)
    print_iterable(items)


def filter_even_id(users: collections_abc.Iterable[User]) -> collections_abc.Iterator[User]:
    for user in users:
        if user.id % 2 == 0:
            continue
        yield user


def run_filter() -> None:
    users = list(gen_fake_users())
    print_iterable(users)

    print("-" * 100)

    filtered_users = filter_even_id(users)
    print_iterable(filtered_users)


T = typing.TypeVar("T")


def safe_next(iterator: collections_abc.Iterator[T]) -> typing.Optional[T]:
    try:
        return next(iterator)
    except StopIteration:
        return None


def merge_emails(
    email_iter1: collections_abc.Iterator[UserEmail],
    email_iter2: collections_abc.Iterator[UserEmail],
) -> collections_abc.Iterator[UserEmail]:
    email1: typing.Optional[UserEmail] = None
    email2: typing.Optional[UserEmail] = None

    while True:
        email1 = email1 or safe_next(email_iter1)
        if email1 is None:
            yield from email_iter2
            return
        email2 = email2 or safe_next(email_iter2)
        if email2 is None:
            yield from email_iter1
            return

        if email2.user_id > email1.user_id:
            yield email1
            email1 = None
        else:
            yield email2
            email2 = None


def run_merge_iterators() -> None:
    emails1 = gen_fake_user_emails()
    emails2 = gen_fake_user_emails()

    emails = merge_emails(emails1, emails2)
    print_iterable(emails)


def merge_users(
    user_iter1: collections_abc.Iterator[User],
    user_iter2: collections_abc.Iterator[User],
) -> collections_abc.Iterator[User]:
    user1: typing.Optional[User] = None
    user2: typing.Optional[User] = None

    while True:
        user1 = user1 or safe_next(user_iter1)
        if user1 is None:
            yield from user_iter2
            return
        user2 = user2 or safe_next(user_iter2)
        if user2 is None:
            yield from user_iter1
            return

        if user1.id > user2.id:
            yield user2
            user2 = None
        elif user1.id < user2.id:
            yield user1
            user1 = None
        else:
            yield User(id=user1.id, emails=user1.emails + user2.emails)
            user1 = None
            user2 = None


def run_merge_objects() -> None:
    users1 = gen_fake_users()
    users2 = gen_fake_users()

    emails = merge_users(users1, users2)
    print_iterable(emails)


run_merge_objects()


if __name__ == "__main__":
    ...
