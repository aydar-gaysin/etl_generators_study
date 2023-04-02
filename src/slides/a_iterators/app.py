import collections.abc as collections_abc
import typing

import utils.profilers as profiler_utils

T = typing.TypeVar("T")

SIZE = 10_000_000


@profiler_utils.profile
def for_in_range() -> None:
    for _ in range(SIZE):
        ...


@profiler_utils.profile
def run_through_iterable(items: collections_abc.Iterable[typing.Any], name: str) -> None:
    print(name)
    for _ in items:
        ...
    print("-" * 100)


def yield_from_layer(items: collections_abc.Iterable[T]) -> collections_abc.Iterator[T]:
    yield from items


def yield_for_layer(items: collections_abc.Iterable[T]) -> collections_abc.Iterator[T]:
    yield from items


def expression_layer(items: collections_abc.Iterable[T]) -> collections_abc.Iterator[T]:
    return (item for item in items)


# @profiler_utils.profile
def range_generator() -> collections_abc.Iterator[int]:
    yield from range(SIZE)


# @profiler_utils.profile
def list_generator() -> list[int]:
    return list(range(SIZE))


def run_baseline() -> None:
    for_in_range()
    print("-" * 100)


def run_basic() -> None:
    run_baseline()
    run_through_iterable(range(SIZE), "range")


def run_different_iteration_styles() -> None:
    run_baseline()

    run_through_iterable(
        yield_from_layer(yield_from_layer(yield_from_layer(yield_from_layer(yield_from_layer(range(SIZE)))))),
        "range with 5 yield from layers",
    )

    run_through_iterable(
        yield_for_layer(yield_for_layer(yield_for_layer(yield_for_layer(yield_for_layer(range(SIZE)))))),
        "range with 5 yield for layers",
    )

    run_through_iterable(
        expression_layer(expression_layer(expression_layer(expression_layer(expression_layer(range(SIZE)))))),
        "range with 5 generator expression layers",
    )


def run_range_vs_list() -> None:
    run_baseline()

    gen1 = range_generator()
    run_through_iterable(gen1, "yield from range")

    gen2 = list_generator()
    run_through_iterable(gen2, "yield from list")


if __name__ == "__main__":
    run_range_vs_list()
