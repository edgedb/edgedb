import asyncio
import enum
import pickle
import time

from typing import Dict
from edb.server.protocol import binary, pg_ext
from edb.server.args import ServerEndpointSecurityMode

class StreamOp(enum.IntEnum):
    Open = 0
    Data = 1
    Close = 2
    EOF = 3
    ConnectionLost = 4
    PauseWriting = 5
    ResumeWriting = 6


class SubProtocol(enum.IntEnum):
    HTTP = 0
    Postgres = 1
    EdgeDB = 2


PACKET_HEADER_LEN = 13  # Length of the packet header in bytes


class MultiplexTransport(asyncio.WriteTransport):
    def __init__(self, underlying_transport, stream_id):
        self.underlying_transport = underlying_transport
        self.stream_id = stream_id

    def write(self, data):
        # Construct the StreamOp.Data packet
        packet = (
            StreamOp.Data.value.to_bytes(1, 'big') +
            self.stream_id.to_bytes(8, 'big') +
            len(data).to_bytes(4, 'big') +
            data
        )
        self.underlying_transport.write(packet)

    def write_eof(self):
        # Construct the StreamOp.EOF packet
        packet = (
            StreamOp.EOF.value.to_bytes(1, 'big') +
            self.stream_id.to_bytes(8, 'big') +
            (0).to_bytes(4, 'big')
        )
        self.underlying_transport.write(packet)

    def close(self):
        # Construct the StreamOp.Close packet
        packet = (
            StreamOp.Close.value.to_bytes(1, 'big') +
            self.stream_id.to_bytes(8, 'big') +
            (0).to_bytes(4, 'big')
        )
        self.underlying_transport.write(packet)

    def abort(self):
        # Construct the StreamOp.ConnectionLost packet
        packet = (
            StreamOp.ConnectionLost.value.to_bytes(1, 'big') +
            self.stream_id.to_bytes(8, 'big') +
            (0).to_bytes(4, 'big')
        )
        self.underlying_transport.write(packet)

    def get_extra_info(self, name, default=None):
        return default

    def is_closing(self):
        return self.underlying_transport.is_closing()

    def get_write_buffer_size(self):
        return self.underlying_transport.get_write_buffer_size()

    def can_write_eof(self):
        return True

    def pause_writing(self):
        # Construct the StreamOp.PauseWriting packet
        packet = (
            StreamOp.PauseWriting.value.to_bytes(1, 'big') +
            self.stream_id.to_bytes(8, 'big') +
            (0).to_bytes(4, 'big')
        )
        self.underlying_transport.write(packet)

    def resume_writing(self):
        # Construct the StreamOp.ResumeWriting packet
        packet = (
            StreamOp.ResumeWriting.value.to_bytes(1, 'big') +
            self.stream_id.to_bytes(8, 'big') +
            (0).to_bytes(4, 'big')
        )
        self.underlying_transport.write(packet)


class MultiplexProtocol(asyncio.Protocol):
    def __init__(self):
        self.streams: Dict[int, asyncio.Protocol] = {}
        self._buffer = b''
        self._current_stream_id = None
        self._current_op = None
        self._current_length = None
        self._bytes_needed = PACKET_HEADER_LEN  # Initial bytes needed for header
        self._open_message_buffer = b''

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self._buffer += data
        while len(self._buffer) >= self._bytes_needed:
            if self._current_stream_id is None:
                # We're starting a new packet
                self._current_op = StreamOp(self._buffer[0])
                self._current_stream_id = int.from_bytes(self._buffer[1:9], 'big')
                self._current_length = int.from_bytes(self._buffer[9:PACKET_HEADER_LEN], 'big')
                self._buffer = self._buffer[PACKET_HEADER_LEN:]
                self._bytes_needed = self._current_length
            else:
                # We're continuing an existing packet
                payload = self._buffer[:self._bytes_needed]
                self._buffer = self._buffer[self._bytes_needed:]

                if self._current_op == StreamOp.Open:
                    self._open_message_buffer += payload
                    if len(self._open_message_buffer) == self._current_length:
                        self._handle_open_stream(self._open_message_buffer)
                        self._open_message_buffer = b''
                elif self._current_op == StreamOp.Data:
                    if self._current_stream_id in self.streams:
                        self.streams[self._current_stream_id].data_received(payload)
                elif self._current_op == StreamOp.Close:
                    if self._current_stream_id in self.streams:
                        self.streams[self._current_stream_id].connection_lost(None)
                        del self.streams[self._current_stream_id]
                elif self._current_op == StreamOp.EOF:
                    if self._current_stream_id in self.streams:
                        self.streams[self._current_stream_id].eof_received()
                elif self._current_op == StreamOp.ConnectionLost:
                    if self._current_stream_id in self.streams:
                        self.streams[self._current_stream_id].connection_lost(Exception("Abrupt connection loss"))
                        del self.streams[self._current_stream_id]
                elif self._current_op == StreamOp.PauseWriting:
                    if self._current_stream_id in self.streams:
                        self.streams[self._current_stream_id].pause_writing()
                elif self._current_op == StreamOp.ResumeWriting:
                    if self._current_stream_id in self.streams:
                        self.streams[self._current_stream_id].resume_writing()

                self._reset_packet_state()

    def create_sub_protocol(self, sub_protocol: SubProtocol):
        connection_made_at = time.monotonic()

        if sub_protocol == SubProtocol.Binary:
            return binary.new_edge_connection(
                self.server,
                self.tenant,
                external_auth=True,
                connection_made_at=connection_made_at,
            )
        elif sub_protocol == SubProtocol.Http:
            return HTTPPickleProtocol(
                self.server,
            )
        elif sub_protocol == SubProtocol.Postgres:
            return pg_ext.new_pg_connection(
                self.server,
                None,
                ServerEndpointSecurityMode.Optional,
                connection_made_at=connection_made_at,
            )
        else:
            raise ValueError(f"Unknown sub-protocol: {sub_protocol}")

    def _handle_open_stream(self, payload):
        if len(payload) < 1:
            # Error handling: not enough data for sub-protocol
            self._reset_packet_state()
            return
        sub_protocol = SubProtocol(payload[0])
        new_protocol = self.create_sub_protocol(sub_protocol)
        new_transport = MultiplexTransport(self.transport, self._current_stream_id)
        self.streams[self._current_stream_id] = new_protocol
        new_protocol.connection_made(new_transport)

    def _reset_packet_state(self):
        self._current_stream_id = None
        self._current_op = None
        self._current_length = None
        self._bytes_needed = PACKET_HEADER_LEN

    def connection_lost(self, exc):
        for stream in self.streams.values():
            stream.connection_lost(exc)
        self.streams.clear()

    def eof_received(self):
        for stream in self.streams.values():
            stream.eof_received()
        return False  # We want the transport to close itself


class HTTPPickleProtocol(asyncio.Protocol):
    def __init__(self):
        self.buffer = b''
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.buffer += data

    def eof_received(self):
        try:
            unpickled_data = pickle.loads(self.buffer)
            if isinstance(unpickled_data, dict):
                self.process_pickled_dict(unpickled_data)
            else:
                print("Received data is not a dictionary")
        except pickle.UnpicklingError:
            print("Error unpickling received data")
        finally:
            self.transport.close()

    def process_pickled_dict(self, data):
        # Stub function to be implemented later
        print("Received pickled dictionary:", data)

    def connection_lost(self, exc):
        if exc:
            print(f"Connection lost due to error: {exc}")
        else:
            print("Connection closed")
