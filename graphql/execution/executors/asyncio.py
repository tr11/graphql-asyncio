from asyncio import Future, get_event_loop, iscoroutine, ensure_future
from inspect import isasyncgen

from aioreactive.core import AsyncIteratorObserver, subscribe
from aioreactive.core.observables import AsyncObservable
from promise import Promise


class AsyncioExecutor(object):

    def __init__(self, loop=None):
        if loop is None:
            loop = get_event_loop()
        self.loop = loop
        self.futures = []
        self.return_promise = False

    def execute(self, fn, *args, **kwargs):
        result = fn(*args, **kwargs)

        if isasyncgen(result):
            return AsyncObservable.from_async_iterable(result)
        elif isinstance(result, AsyncObservable):
            return result
            # async def to_async():
            #     obv = AsyncIteratorObserver()
            #     async with subscribe(result, obv) as _:
            #         async for x in obv:
            #             yield x
            # return to_async()
        elif isinstance(result, Future) or iscoroutine(result):
            future = ensure_future(result, loop=self.loop)
            return Promise.resolve(future)
        return result
