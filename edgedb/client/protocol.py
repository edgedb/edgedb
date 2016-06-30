##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncio
import enum
import json


from . import exceptions
from .future import create_future


class ConnectionState(enum.Enum):
    NOT_CONNECTED = 0
    NEW = 1
    AUTHENTICATING = 2
    READY = 3


class Protocol(asyncio.Protocol):
    def __init__(self, address, connect_waiter,
                 user, password, database, loop):
        self._address = address
        self._user = user
        self._password = password
        self._database = database
        self._loop = loop
        self._address = address
        self._hash = (self._address, self._database)

        self._connect_waiter = connect_waiter
        self._waiter = None
        self._state = ConnectionState.NOT_CONNECTED

    def connection_made(self, transport):
        self.transport = transport
        self._init_connection()

    def connection_lost(self, exc):
        self.transport.close()

    def data_received(self, data):
        msg = json.loads(data.decode('utf-8'))
        self.process_message(msg)

    def execute(self, query):
        msg = {
            '__type__': 'query',
            'query': query
        }

        self._waiter = create_future(self._loop)
        self.send_message(msg)

        return self._waiter

    def execute_script(self, script):
        msg = {
            '__type__': 'script',
            'script': script
        }

        self._waiter = create_future(self._loop)
        self.send_message(msg)

        return self._waiter

    def send_message(self, message):
        self.transport.write(json.dumps(message).encode('utf-8'))

    def process_message(self, message):
        if message['__type__'] == 'authresult':
            if not self._connect_waiter.cancelled():
                self._connect_waiter.set_result(None)
            self._connect_waiter = None

        elif message['__type__'] == 'error':
            if self._connect_waiter is not None:
                self._connect_waiter.set_exception(
                    exceptions.Error(message['data']['message'],
                                     code=message['data']['code']))
                self._connect_waiter = None
            elif self._waiter is not None:
                self._waiter.set_exception(
                    exceptions.Error(message['data']['message'],
                                     code=message['data']['code']))
                self._waiter = None

        elif message['__type__'] == 'result':
            if self._waiter is not None:
                self._waiter.set_result(message['result'])
            self._waiter = None

    def _init_connection(self):
        msg = {
            '__type__': 'init',
            'user': self._user,
            'database': self._database
        }

        self.send_message(msg)
        self.state = ConnectionState.AUTHENTICATING
