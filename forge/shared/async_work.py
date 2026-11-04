"""Bounded async workers: no task per input and no unbounded result buffer."""

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class _Result[T]:
    value: T


async def bounded_map[T, R](
    inputs: AsyncIterable[T],
    function: Callable[[T], Awaitable[R]],
    *,
    workers: int,
) -> AsyncIterator[R]:
    if workers < 1:
        raise ValueError("workers must be positive")
    pending: asyncio.Queue[_Result[T] | None] = asyncio.Queue(workers * 2)
    results: asyncio.Queue[_Result[R] | BaseException | None] = asyncio.Queue(workers * 2)

    async def produce() -> None:
        try:
            async for value in inputs:
                await pending.put(_Result(value))
        except Exception as error:
            await results.put(error)
        else:
            for _ in range(workers):
                await pending.put(None)

    async def consume() -> None:
        try:
            while (value := await pending.get()) is not None:
                try:
                    result = await function(value.value)
                except Exception as error:
                    await results.put(error)
                else:
                    await results.put(_Result(result))
        except asyncio.CancelledError:
            raise
        else:
            await results.put(None)

    tasks = [
        asyncio.create_task(produce()),
        *(asyncio.create_task(consume()) for _ in range(workers)),
    ]
    finished = 0
    try:
        while finished < workers:
            result = await results.get()
            if isinstance(result, BaseException):
                raise result
            if result is None:
                finished += 1
            else:
                yield result.value
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
