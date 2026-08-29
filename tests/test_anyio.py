import shutil

import pytest
from lmdb import Environment

from anyio_lmdb import AsyncEnvironment, wrap


class LMDBTest:
    @classmethod
    def teardown_method(cls):
        shutil.rmtree("_test")

    def raw_env(self) -> Environment:
        return Environment("_test")

    def env(self) -> AsyncEnvironment:
        return AsyncEnvironment.create("_test")


@pytest.mark.anyio
class TestEnviornment(LMDBTest):
    async def test_wrapper(self) -> None:
        aenv = wrap(self.raw_env())
        assert isinstance(aenv, AsyncEnvironment)

    async def test_context_manager_create(self) -> None:
        async with self.env() as aenv:
            await aenv.stat()

    async def test_context_manager_wrapped(self) -> None:
        async with wrap(self.raw_env()) as aenv:
            await aenv.stat()

    async def test_stat(self) -> None:
        async with self.env() as aenv:
            st = await aenv.stat()
            assert "entries" in st
            assert "psize" in st


@pytest.mark.anyio
class TestTxnTest(LMDBTest):
    async def test_put_get(self) -> None:
        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"hello", b"world")

            async with aenv.begin() as txn:
                val = await txn.get(b"hello")
                assert val == b"world"

    async def test_delete(self) -> None:
        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"k", b"v")
            async with aenv.begin(write=True) as txn:
                assert await txn.delete(b"k")
                assert (await txn.get(b"k")) is None

    async def test_replace(self) -> None:
        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"k", b"old")
            async with aenv.begin(write=True) as txn:
                old = await txn.replace(b"k", b"new")
                assert old == b"old"
            async with aenv.begin() as txn:
                assert (await txn.get(b"k")) == b"new"

    async def test_pop(self) -> None:
        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"k", b"v")
            async with aenv.begin(write=True) as txn:
                val = await txn.pop(b"k")
                assert val == b"v"
                assert (await txn.get(b"k")) is None

    async def test_stat(self) -> None:
        env = self.raw_env()
        async with wrap(env) as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"k", b"v")
            async with aenv.begin() as txn:
                st = await txn.stat(env.open_db())
                assert st["entries"] == 1

    async def test_id_sync(self) -> None:
        """id() is a sync accessor — should return directly."""
        async with self.env() as aenv, aenv.begin() as txn:
            tid = txn.id()
            assert isinstance(tid, int)

    async def test_abort_on_exception(self) -> None:
        async with self.env() as aenv:
            try:
                async with aenv.begin(write=True) as txn:
                    await txn.put(b"k", b"v")
                    raise ValueError("boom")
            except ValueError:
                pass
            # The put should have been rolled back
            async with aenv.begin() as txn:
                assert (await txn.get(b"k")) is None


@pytest.mark.anyio
class TestAsyncCursor(LMDBTest):
    async def test_cursor_put_and_navigate(self) -> None:
        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn, txn.cursor() as cur:
                await cur.put(b"a", b"1")
                await cur.put(b"b", b"2")
                await cur.put(b"c", b"3")

            async with aenv.begin() as txn, txn.cursor() as cur:
                assert await cur.first()
                assert cur.key() == b"a"
                assert await cur.next()
                assert cur.key() == b"b"
                assert await cur.last()
                assert cur.key() == b"c"
                assert await cur.prev()
                assert cur.key() == b"b"

    async def test_set_key(self) -> None:

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"x", b"10")
                await txn.put(b"y", b"20")
            async with aenv.begin() as txn, txn.cursor() as cur:
                assert await cur.set_key(b"y")
                assert cur.value() == b"20"
                assert (await cur.set_key(b"z")) is False

    async def test_set_range(self) -> None:

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"aa", b"1")
                await txn.put(b"cc", b"3")
            async with aenv.begin() as txn, txn.cursor() as cur:
                assert await cur.set_range(b"bb")
                assert cur.key() == b"cc"

    async def test_iternext(self) -> None:

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"a", b"1")
                await txn.put(b"b", b"2")
                await txn.put(b"c", b"3")
            async with aenv.begin() as txn, txn.cursor() as cur:
                await cur.first()
                items = await cur.iternext()
                assert items == [
                    (b"a", b"1"),
                    (b"b", b"2"),
                    (b"c", b"3"),
                ]

    async def test_iterprev(self) -> None:

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"a", b"1")
                await txn.put(b"b", b"2")
            async with aenv.begin() as txn, txn.cursor() as cur:
                await cur.last()
                items = await cur.iterprev()
                assert items == [(b"b", b"2"), (b"a", b"1")]

    async def test_iternext_keys_only(self) -> None:

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"x", b"1")
                await txn.put(b"y", b"2")
            async with aenv.begin() as txn, txn.cursor() as cur:
                await cur.first()
                keys = await cur.iternext(keys=True, values=False)
                assert keys == [b"x", b"y"]

    @pytest.mark.skip(
        "TODO:  lmdb.DbsFullError: mdb_dbi_open: MDB_DBS_FULL: Environment "
        "maxdbs limit reached"
    )
    async def test_count(self) -> None:

        async with self.env() as aenv:
            db = aenv._env.open_db(b"dup", dupsort=True)
            async with aenv.begin(write=True, db=db) as txn:
                await txn.put(b"k", b"a")
                await txn.put(b"k", b"b")
                await txn.put(b"k", b"c")
            async with aenv.begin(db=db) as txn, txn.cursor() as cur:
                await cur.set_key(b"k")
                assert (await cur.count()) == 3

    async def test_cursor_delete(self) -> None:

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"a", b"1")
                await txn.put(b"b", b"2")
            async with aenv.begin(write=True) as txn, txn.cursor() as cur:
                await cur.set_key(b"a")
                await cur.delete()
            async with aenv.begin() as txn:
                assert (await txn.get(b"a")) is None
                assert await txn.get(b"b") == b"2"

    async def test_key_value_item_sync(self) -> None:
        """key(), value(), item() should be direct (not awaitable)."""

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"k", b"v")
            async with aenv.begin() as txn, txn.cursor() as cur:
                await cur.first()
                # These are sync — return values directly
                assert cur.key() == b"k"
                assert cur.value() == b"v"
                assert cur.item() == (b"k", b"v")

    async def test_cursor_aiter(self) -> None:

        async with self.env() as aenv:
            async with aenv.begin(write=True) as txn:
                await txn.put(b"a", b"1")
                await txn.put(b"b", b"2")

            async with aenv.begin() as txn, txn.cursor() as cur:
                async for k, v in cur:
                    match k:
                        case b"a":
                            assert v == b"1"
                        case b"b":
                            assert v == b"2"
