import edb.server._conn_pool
import asyncio
import threading
import time
import typing

class ConnectionFactory(typing.Protocol):
    async def connect(self, db: str):
        ...
    async def disconnect(self, conn):
        ...
    async def reconnect(self, conn, db: str):
        ...

C = typing.TypeVar('C')

class ConnPool(typing.Generic[C]):
    connection_factory: ConnectionFactory
    pool: edb.server._conn_pool.ConnPool
    loop: asyncio.BaseEventLoop
    completion: asyncio.Future

    def __init__(self, connection_factory: C):
        self.connection_factory = connection_factory
        self.loop = None
        self.pool = None
        self.completion = None

    def _callback(self, kind: int, response_id: int, *args):
        """Receives the callback from the Rust connection pool.

        Required to call pool._respond with the result of this callback
        """
        if self.loop.is_closed():
            return False
        else:
            self.loop.call_soon_threadsafe(self.loop.create_task, self._perform_async(kind, response_id, *args))
            return True

    async def _perform_async(self, kind: int, response_id: int, *args):
        if kind == 0:
            response = await self.connection_factory.connect(*args)
        elif kind == 1:
            response = await self.connection_factory.disconnect(*args)
        elif kind == 2:
            response = await self.connection_factory.reconnect(*args)
        self.pool._respond(response_id, response)

    def _thread_main(self):
        self.pool.run()
        if not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.completion.set_result, True)

    async def run(self):
        if self.pool != None:
            raise RuntimeError(
                f'pool already started'
            ) from None

        self.loop = asyncio.get_event_loop()
        self.pool = edb.server._conn_pool.ConnPool(self._callback)
        self.completion = self.loop.create_future()
        threading.Thread(target = self._thread_main).start()
        try:
            await self.completion
        except asyncio.exceptions.CancelledError:
            self.pool.halt()
        self.pool = None

async def main():
    class Factory:
        async def connect(self, db):
            await asyncio.sleep(0.2)
            return f"Connection '{db}'"
        async def disconnect(self, conn):
            await asyncio.sleep(0.2)
            return
        async def reconnect(self, conn, db):
            await asyncio.sleep(0.2)
            return f"Connection '{db}'"

    pool = ConnPool(Factory())
    await pool.run()

asyncio.run(main(), debug=True)
