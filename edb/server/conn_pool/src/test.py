import logging

# logging.basicConfig(level=logging.INFO)
# logging.getLogger("edb.server.connpool").setLevel(1)

# ruff: noqa: E402
import edb.server._conn_pool
import asyncio
import uvloop
import threading
import typing
import time
import sys

# Connections must be hashable because we use them to reverse-lookup
# an internal ID.
C = typing.TypeVar("C", bound=typing.Hashable)


class ConnectionFactory(typing.Protocol[C]):
    """The async interface to create and destroy database connections.

    All connections returned from successful calls to `connect` or reconnect
    are guaranteed to be `disconnect`ed or `reconnect`ed."""

    async def connect(self, db: str) -> C:
        """Create a new connection asynchronously.

        This method must retry exceptions internally. If an exception is thrown
        from this method, the database is considered to be failed."""
        ...

    async def disconnect(self, conn: C):
        """Gracefully disconnect a connection asynchronously.

        If an exception is thrown from this method, the connection is simply
        forgotten."""
        ...

    async def reconnect(self, conn: C, db: str) -> C:
        """Reconnects a connection to the given database. If this is not
        possible, it is permissable to return a new connection and gracefully
        disconnect the other connection in parallel or in the background.

        This method must retry exceptions internally. If an exception is thrown
        from this method, the database is considered to be failed."""
        ...


class ConnPool(ConnectionFactory[C]):
    _connection_factory: ConnectionFactory[C]
    _pool: edb.server._conn_pool.ConnPool
    _loop: asyncio.AbstractEventLoop
    _completion: asyncio.Future[bool]
    _ready: asyncio.Future[bool]
    _active_conns: set[C]

    def __init__(self, connection_factory: ConnectionFactory[C]):
        self._connection_factory = connection_factory
        self._loop = asyncio.get_event_loop()
        self._pool = None
        self._completion = self._loop.create_future()
        self._ready = self._loop.create_future()
        self._active_conns = set()

    def _callback(self, args0, args) -> bool:
        """Receives the callback from the Rust connection pool.

        Required to call pool._respond on the main thread with the result of
        this callback.
        """
        (kind, response_id) = args0
        if self._loop.is_closed():
            return False
        else:
            self._loop.call_soon_threadsafe(
                self._loop.create_task,
                self._perform_async(kind, response_id, *args),
            )
            return True

    async def _perform_async(self, kind: int, response_id: int, *args):
        """Delegates the callback from Rust to the appropriate connection
        factory method."""
        if kind == 0:
            response = await self._connection_factory.connect(*args)
        elif kind == 1:
            response = await self._connection_factory.disconnect(*args)
        elif kind == 2:
            response = await self._connection_factory.reconnect(*args)
        self._pool._respond(response_id, response)

    def _thread_main(self):
        self._loop.call_soon_threadsafe(self._ready.set_result, True)
        self._pool.run_and_block()
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._completion.set_result, True)

    async def run(self):
        """Creates a long-lived task that manages the connection pool. Required
        before any connections may be acquired."""
        if self._pool is not None:
            raise RuntimeError(f"pool already started") from None

        self._pool = edb.server._conn_pool.ConnPool(self._callback)
        threading.Thread(target=self._thread_main, daemon=True).start()
        try:
            await self._completion
        except asyncio.exceptions.CancelledError:
            self._pool.halt()
        self._pool = None

    async def acquire(self, db) -> C:
        """Acquire a connection from the database. This connection must be
        released."""
        await self._ready
        future = self._loop.create_future()
        # Note that this callback is called on the internal pool's thread
        self._pool.acquire(
            db,
            lambda res: self._loop.call_soon_threadsafe(future.set_result, res),
        )
        conn = await future
        self._active_conns.add(conn)
        return conn

    def release(self, _db, conn, discard=False):
        """Releases a connection back into the pool, discarding or returning it
        in the background."""
        self._active_conns.remove(conn)
        self._pool.release(conn, discard)
        pass


async def main():
    class Factory:
        def __init__(self) -> None:
            self.id = 0

        async def connect(self, db):
            print(f"Python Factory.connect db={db}")
            await asyncio.sleep(0.2)
            self.id += 1
            return f"Connection '{db}' #{self.id}"

        async def disconnect(self, conn):
            print(f"Python Factory.disconnect conn={conn}")
            await asyncio.sleep(0.2)
            return

        async def reconnect(self, conn, db):
            print(f"Python Factory.reconnect conn={conn} db={db}")
            await asyncio.sleep(0.2)
            return f"Connection '{db}' (was {conn})"

    pool = ConnPool(Factory())
    asyncio.create_task(pool.run())
    conn = await pool.acquire("test")
    print("Python main acquired a connection:", conn)
    pool.release("test", conn)
    conn = await pool.acquire("test")
    print("Python main acquired a connection:", conn)
    pool.release("test", conn)



# uvloop.run(main(), debug=True)

class Factory:
    async def connect(self, x):
        return x

    async def disconnect(self, x):
        pass

async def conn(pool, x):
    conn = await pool.acquire(f"db{x%10}")
    pool.release(f"db{x%10}", conn=conn)

async def main():
    pool = ConnPool(Factory())
    task = asyncio.create_task(pool.run())
    # pool = edb.server.connpool.Pool(connect=connect, disconnect=disconnect, max_capacity=10)
    print("Spawn")
    async with asyncio.TaskGroup() as g:
        for x in range(0, 10000):
            g.create_task(conn(pool, x))
    print("Done")

start_time = time.perf_counter()
loop = uvloop.run(main())
end_time = time.perf_counter()
print(f"{(end_time - start_time) * 1000}ms")
