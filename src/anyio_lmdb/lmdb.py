# Forked from lmdb and modified to work via anyio

"""
Async wrappers for py-lmdb via :func:`anyio.to_thread.run_sync`.
"""

import functools
import sys
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Coroutine,
    Generator,
    Sequence,
)
from os import PathLike
from types import TracebackType
from typing import Any, AnyStr, Generic, TypeVar

import anyio
from anyio.to_thread import run_sync
from lmdb import Cursor, Environment, Transaction

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

T = TypeVar("T")


def wrap(env: Environment) -> "AsyncEnvironment":
    """Wrap an :class:`lmdb.Environment` for async use.

    *env* is passed to :meth:`anyio.to_thread.run_sync`.
    """
    return AsyncEnvironment(env)


class _AsyncContextWrapper(Generic[T]):
    """Wraps a coroutine so it can be used as both ``await`` and ``async with``

    Supports::

        txn = await aenv.begin(write=True)      # just await
        async with aenv.begin(write=True) as txn:  # context manager
    """

    __slots__ = ("_coro", "_result")

    def __init__(self, coro: Coroutine[Any, Any, T]):
        self._coro = coro
        self._result = None

    def __await__(self) -> Generator[Any, Any, T]:
        return self._coro.__await__()

    async def __aenter__(self) -> T:
        self._result = await self._coro
        return self._result

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return await self._result.__aexit__(exc_type, exc_val, exc_tb)


# ---------------------------------------------------------------------------
# Proxy method factories
# ---------------------------------------------------------------------------


def _sync_method(sync):
    """Return a method that calls *sync* directly, without an executor."""

    @functools.wraps(sync)
    def method(self, *args, **kwargs):
        return sync(getattr(self, self._WRAPS), *args, **kwargs)

    return method


def _async_method(sync):
    """Return a coroutine method that calls :meth:`anyio.to_thread.run_sync`"""

    @functools.wraps(sync)
    async def method(self, *args, **kwargs):
        return await run_sync(
            functools.partial(
                sync, getattr(self, self._WRAPS), *args, **kwargs
            ),
        )

    return method


def _async_method_locked(sync):
    """Like :func:`_async_method`, but acquires ``self._lock`` first."""

    @functools.wraps(sync)
    async def method(self, *args, **kwargs):
        return await run_sync(
            functools.partial(
                sync, getattr(self, self._WRAPS), *args, **kwargs
            ),
            limiter=self._lock,
        )

    return method


def _collect_locked(sync):
    """Like :func:`_async_method_locked`, but *sync* returns an iterator
    consumed in the executor and returned as a list."""

    @functools.wraps(sync)
    async def method(self, *args, **kwargs):

        return await run_sync(
            lambda: list(sync(getattr(self, self._WRAPS), *args, **kwargs)),
            limiter=self._lock,
        )

    return method


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------

O_0755 = int("0755", 8)


class AsyncEnvironment:
    """Async wrapper for :py:class:`lmdb.Environment`.

    Created by :py:func:`wrap`.  All methods of the underlying
    :py:class:`~lmdb.Environment` are available and are dispatched to an
    executor, except for the low-overhead accessors ``path()``,
    ``max_key_size()``, ``max_readers()``, and ``flags()``, which are called
    directly.

    Supports ``async with`` for lifetime management — the
    environment is closed on exit.
    """

    __slots__ = ("_env",)

    _WRAPS = "_env"

    def __init__(self, env: Environment):
        self._env = env

    @classmethod
    def create(
        cls,
        path: AnyStr | PathLike[AnyStr],
        map_size: int = 10485760,
        subdir: bool = True,
        readonly: bool = False,
        metasync: bool = True,
        sync: bool = True,
        map_async: bool = False,
        mode: int = O_0755,
        create: bool = True,
        readahead: bool = True,
        writemap: bool = False,
        meminit: bool = True,
        max_readers: int = 126,
        max_dbs: int = 0,
        max_spare_txns: int = 1,
        lock: bool = True,
    ) -> Self:
        """Small and easy shortcut to cut right into utilizing
        AsyncEnviornments immediately."""
        return cls(
            Environment(
                path,
                map_size,
                subdir,
                readonly,
                metasync,
                sync,
                map_async,
                mode,
                create,
                readahead,
                writemap,
                meminit,
                max_readers,
                max_dbs,
                max_spare_txns,
                lock,
            )
        )

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        await self.close()

    # -- methods that return wrapped objects -------------------------------

    def begin(self, *args, **kwargs):
        """Start a new transaction, returning an :py:class:`AsyncTransaction`.

        Accepts the same arguments as :py:meth:`lmdb.Environment.begin`.
        Can be used with ``await`` or ``async with``::

            async with aenv.begin(write=True) as txn:
                await txn.put(b'key', b'value')
        """

        async def _begin() -> AsyncTransaction:
            txn = await run_sync(
                functools.partial(self._env.begin, *args, **kwargs)
            )
            return AsyncTransaction(txn)

        return _AsyncContextWrapper(_begin())

    # -- proxied methods --------------------------------------------------

    path = _sync_method(Environment.path)
    max_key_size = _sync_method(Environment.max_key_size)
    max_readers = _sync_method(Environment.max_readers)
    flags = _sync_method(Environment.flags)

    stat = _async_method(Environment.stat)
    info = _async_method(Environment.info)
    close = _async_method(Environment.close)
    copy = _async_method(Environment.copy)
    copyfd = _async_method(Environment.copyfd)
    sync = _async_method(Environment.sync)
    readers = _async_method(Environment.readers)
    reader_check = _async_method(Environment.reader_check)
    set_mapsize = _async_method(Environment.set_mapsize)
    open_db = _async_method(Environment.open_db)
    dbs = _async_method(Environment.dbs)

    # -- attribute fallback -----------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._env, name)
        if callable(attr):
            raise AttributeError(name)
        return attr


class AsyncTransaction:
    """Async wrapper for :py:class:`lmdb.Transaction`.

    All methods of the underlying :py:class:`~lmdb.Transaction` are available.
    Most are dispatched to an executor; ``id()`` is called directly.

    An :py:class:`anyio.CapacityLimiter` serializes all operations dispatched
    through this transaction, including operations on its cursors.  This makes
    joining tasks safe on the same transaction.

    Supports ``async with`` — write transactions are committed on clean exit
    and aborted on exception.
    """

    __slots__ = ("_lock", "_txn")

    _WRAPS = "_txn"

    def __init__(self, txn: Transaction) -> None:
        self._txn = txn
        self._lock = anyio.CapacityLimiter(1)

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, _exc_val, _exc_tb):
        async with self._lock:
            if exc_type:
                self._txn.abort()
            else:
                await run_sync(self._txn.commit)

    # -- methods that return wrapped objects -------------------------------

    def cursor(self, *args, **kwargs):
        """Open a cursor, returning an :py:class:`AsyncCursor`.

        Accepts the same arguments as :py:meth:`lmdb.Transaction.cursor`.
        Can be used with ``await`` or ``async with``::

            async with txn.cursor() as cur:
                await cur.first()
                items = await cur.iternext()
        """

        async def _cursor() -> AsyncCursor:
            async with self._lock:
                cur = await run_sync(
                    functools.partial(self._txn.cursor, *args, **kwargs),
                )
            return AsyncCursor(cur, self._lock)

        return _AsyncContextWrapper(_cursor())

    # -- proxied methods --------------------------------------------------

    id = _sync_method(Transaction.id)

    stat = _async_method_locked(Transaction.stat)
    drop = _async_method_locked(Transaction.drop)
    commit = _async_method_locked(Transaction.commit)
    abort = _async_method_locked(Transaction.abort)
    get = _async_method_locked(Transaction.get)
    put = _async_method_locked(Transaction.put)
    replace = _async_method_locked(Transaction.replace)
    pop = _async_method_locked(Transaction.pop)
    delete = _async_method_locked(Transaction.delete)

    # -- attribute fallback -----------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._txn, name)
        if callable(attr):
            raise AttributeError(name)
        return attr


class AsyncCursor(Generic[T]):
    """Async wrapper for :py:class:`lmdb.Cursor`.

    All methods of the underlying :py:class:`~lmdb.Cursor` are available.
    Most are dispatched to an executor; ``key()``, ``value()``, and
    ``item()`` are called directly.

    Iterator methods (``iternext()``, ``iterprev()``, etc.) are consumed in
    the executor and returned as a list.

    Shares the parent transaction's :py:class:`asyncio.Lock`.

    Supports ``async with`` — the cursor is closed on exit.
    """

    __slots__ = ("_cursor", "_lock")

    _WRAPS = "_cursor"

    def __init__(
        self, cursor: Cursor, lock: anyio.CapacityLimiter | None = None
    ):
        self._cursor = cursor
        self._lock = lock or anyio.CapacityLimiter(1)

    # -- context manager --------------------------------------------------

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc) -> None:
        self._cursor.close()

    # -- proxied methods --------------------------------------------------

    def key(self) -> bytes:
        return self._cursor.key()

    def value(self) -> bytes:
        return self._cursor.value()

    def item(self) -> tuple[bytes, bytes]:
        return self._cursor.item()

    close = _async_method_locked(Cursor.close)
    first = _async_method_locked(Cursor.first)
    first_dup = _async_method_locked(Cursor.first_dup)
    last = _async_method_locked(Cursor.last)
    last_dup = _async_method_locked(Cursor.last_dup)
    prev = _async_method_locked(Cursor.prev)
    prev_dup = _async_method_locked(Cursor.prev_dup)
    prev_nodup = _async_method_locked(Cursor.prev_nodup)
    next = _async_method_locked(Cursor.next)
    next_dup = _async_method_locked(Cursor.next_dup)
    next_nodup = _async_method_locked(Cursor.next_nodup)
    set_key = _async_method_locked(Cursor.set_key)
    set_key_dup = _async_method_locked(Cursor.set_key_dup)
    set_range = _async_method_locked(Cursor.set_range)
    set_range_dup = _async_method_locked(Cursor.set_range_dup)
    delete = _async_method_locked(Cursor.delete)
    count = _async_method_locked(Cursor.count)
    put = _async_method_locked(Cursor.put)
    putmulti = _async_method_locked(Cursor.putmulti)
    replace = _async_method_locked(Cursor.replace)
    pop = _async_method_locked(Cursor.pop)
    get = _async_method_locked(Cursor.get)
    getmulti = _async_method_locked(Cursor.getmulti)

    iternext = _collect_locked(Cursor.iternext)
    iternext_dup = _collect_locked(Cursor.iternext_dup)
    iternext_nodup = _collect_locked(Cursor.iternext_nodup)
    iterprev = _collect_locked(Cursor.iterprev)
    iterprev_dup = _collect_locked(Cursor.iterprev_dup)
    iterprev_nodup = _collect_locked(Cursor.iterprev_nodup)

    # -- attribute fallback -----------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._cursor, name)
        if callable(attr):
            raise AttributeError(name)
        return attr

    # Custom: Vizonex Edition
    async def __anext__(self) -> tuple[bytes, bytes]:
        if not await self.next():
            raise StopAsyncIteration
        return self.item()

    def __aiter__(self) -> Self:
        return self
