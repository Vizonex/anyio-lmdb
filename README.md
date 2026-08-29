# anyio_lmdb
A simple lmdb wrapper for anyio forked from the python lmdb library's aio module. This library is small and easy to use
and was seen by it's author as a possible canidate to add to the [keyspec](https://github.com/Vizonex/keyspec) library (asynchronous msgspec cache library) as it is supported on windows and all other operating systems.


```python
from anyio_lmdb import AsyncEnvironment


async def main():
    async with AsyncEnvironment.create("db", readonly=False) as env:
        async with env.begin() as b:
            await b.put(b"key", b"value")
            v = await b.get(b"key")
            # b'value'
            print(v)


if __name__ == "__main__":
    import anyio

    anyio.run(main)

```



