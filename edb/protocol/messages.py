# mypy: ignore-errors

#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import enum
import io
import typing

from edb.common import binwrapper
from .enums import Cardinality

from . import render_utils


_PAD = 16


class CType:
    pass


class Scalar(CType):

    cname = None

    def __init__(
        self, doc: typing.Optional[str] = None, *, default: typing.Any = None
    ) -> None:
        self.doc = doc
        self.default = default

    def validate(self, val: typing.Any) -> bool:
        raise NotImplementedError

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        raise NotImplementedError

    def dump(self, val: typing.Any, buffer: binwrapper.BinWrapper) -> None:
        raise NotImplementedError

    def render_field(
        self, fieldname: str, buf: render_utils.RenderBuffer
    ) -> None:
        cname = self.cname
        if cname is None:
            raise NotImplementedError

        if self.default and isinstance(self.default, int):
            buf.write(
                f'{cname.ljust(_PAD - 1)} {fieldname} = {self.default:#x};')
        elif self.default:
            buf.write(
                f'{cname.ljust(_PAD - 1)} {fieldname} = {self.default};')
        else:
            buf.write(
                f'{cname.ljust(_PAD - 1)} {fieldname};')


class UInt8(Scalar):

    cname = 'uint8'

    def validate(self, val: typing.Any) -> bool:
        return isinstance(val, int) and (0 <= val <= 255)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        return buffer.read_ui8()

    def dump(self, val: int, buffer: binwrapper.BinWrapper) -> None:
        buffer.write_ui8(val)


class UInt16(Scalar):

    cname = 'uint16'

    def validate(self, val: typing.Any) -> bool:
        return isinstance(val, int) and (0 <= val <= 2 ** 16 - 1)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        return buffer.read_ui16()

    def dump(self, val: int, buffer: binwrapper.BinWrapper) -> None:
        buffer.write_ui16(val)


class UInt32(Scalar):

    cname = 'uint32'

    def validate(self, val: typing.Any) -> bool:
        return isinstance(val, int) and (0 <= val <= 2 ** 32 - 1)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        return buffer.read_ui32()

    def dump(self, val: int, buffer: binwrapper.BinWrapper) -> None:
        buffer.write_ui32(val)


class UInt64(Scalar):

    cname = 'uint64'

    def validate(self, val: typing.Any) -> bool:
        return isinstance(val, int) and (0 <= val <= 2 ** 64 - 1)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        return buffer.read_ui64()

    def dump(self, val: int, buffer: binwrapper.BinWrapper) -> None:
        buffer.write_ui64(val)


class Bytes(Scalar):

    cname = 'bytes'

    def validate(self, val: typing.Any) -> bool:
        return isinstance(val, bytes)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        return buffer.read_len32_prefixed_bytes()

    def dump(self, val: bytes, buffer: binwrapper.BinWrapper) -> None:
        buffer.write_len32_prefixed_bytes(val)


class String(Scalar):

    cname = 'string'

    def validate(self, val: typing.Any) -> bool:
        return isinstance(val, str)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        return buffer.read_len32_prefixed_bytes().decode('utf-8')

    def dump(self, val: str, buffer: binwrapper.BinWrapper) -> None:
        buffer.write_len32_prefixed_bytes(val.encode('utf-8'))


class UUID(Scalar):

    cname = 'uuid'

    def validate(self, val: typing.Any) -> bool:
        return isinstance(val, bytes) and len(val) == 16

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        return buffer.read_bytes(16)

    def dump(self, val: bytes, buffer: binwrapper.BinWrapper) -> None:
        assert isinstance(val, bytes) and len(val) == 16
        buffer.write_bytes(val)


class ArrayOf(CType):

    def __init__(
        self,
        length_in: typing.Type[CType],
        element: typing.Union[CType, typing.Type[Struct]],
        doc: str = None,
    ) -> None:
        self.length_in = length_in()
        self.element = element
        self.doc = doc

    def validate(self, val: typing.Any) -> bool:
        if not isinstance(val, list) or not self.length_in.validate(len(val)):
            return False

        if isinstance(self.element, CType):
            return all(self.element.validate(x) for x in val)
        else:
            return all(isinstance(x, self.element) for x in val)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        length = self.length_in.parse(buffer)
        result = []
        for _ in range(length):
            result.append(self.element.parse(buffer))
        return result

    def dump(self, val: list, buffer: binwrapper.BinWrapper) -> None:
        self.length_in.dump(len(val), buffer)
        for el in val:
            self.element.dump(el, buffer)

    def render_field(
        self, fieldname: str, buf: render_utils.RenderBuffer
    ) -> None:
        self.length_in.render_field(f'num_{fieldname}', buf)
        self.element.render_field(f'{fieldname}[num_{fieldname}]', buf)


class FixedArrayOf(CType):

    def __init__(
        self,
        length: int,
        element: typing.Union[CType, typing.Type[Struct]],
        doc: typing.Optional[str]=None
    ) -> None:
        self.length = length
        self.element = element
        self.doc = doc

    def validate(self, val: typing.Any) -> bool:
        if not isinstance(val, list) or len(val) != self.length:
            return False

        if isinstance(self.element, CType):
            return all(self.element.validate(x) for x in val)
        else:
            return all(isinstance(x, self.element) for x in val)

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        result = []
        for _ in range(self.length):
            result.append(self.element.parse(buffer))
        return result

    def dump(self, val: list, buffer: binwrapper.BinWrapper) -> None:
        assert len(val) == self.length
        self.length_in.dump(self.length, buffer)
        for el in val:
            self.element.dump(el, buffer)

    def render_field(
        self, fieldname: str, buf: render_utils.RenderBuffer
    ) -> None:
        self.element.render_field(f'{fieldname}[{self.length}]', buf)


class EnumOf(CType):

    def __init__(
        self,
        value_in: typing.Type[Scalar],
        enum: typing.Type[enum.Enum],
        doc: typing.Optional[str]=None,
    ) -> None:
        self.value_in = value_in()
        self.enum = enum
        self.doc = doc

    def validate(self, val: typing.Any) -> bool:
        if isinstance(val, self.enum):
            return True
        if not self.value_in.validate(val):
            return False
        try:
            self.enum(val)
        except ValueError:
            return False
        else:
            return True

    def parse(self, buffer: binwrapper.BinWrapper) -> any:
        result = self.value_in.parse(buffer)
        return self.enum(result)

    def dump(self, val: typing.Any, buffer: binwrapper.BinWrapper) -> None:
        self.value_in.dump(val.value, buffer)

    def render_field(
        self, fieldname: str, buf: render_utils.RenderBuffer
    ) -> None:
        typename = f'{self.value_in.cname}<{self.enum.__name__}>'
        buf.write(f'{typename.ljust(_PAD - 1)} {fieldname};')


class Struct:

    _fields: typing.Dict[str, typing.Union[CType, typing.Type[Struct]]] = {}

    def __init_subclass__(cls, *, abstract=False):
        if abstract:
            return

        fields = {}

        for name in cls.__dict__:
            attr = cls.__dict__[name]
            if name.startswith('__') or callable(attr):
                continue

            if not isinstance(attr, CType):
                raise TypeError(
                    f'field {cls.__name__}.{name!r} must be a Type')
            else:
                fields[name] = attr

        cls._fields = fields

    def __init__(self, **args: typing.Any):
        for fieldname in ['mtype', 'message_length']:
            if fieldname in args:
                raise ValueError(
                    f'cannot construct instance of {type(self).__name__}: '
                    f'{fieldname!r} field is not supposed to be passed to '
                    f'the constructor')

        for fieldname, field in type(self)._fields.items():
            if fieldname in ['mtype', 'message_length']:
                continue
            try:
                arg = args[fieldname]
            except KeyError:
                raise ValueError(
                    f'cannot construct instance of {type(self).__name__}: '
                    f'the {fieldname!r} field is missing')
            if (
                isinstance(field, CType) and not field.validate(arg) or
                isinstance(field, type) and not isinstance(arg, field)
            ):
                raise ValueError(
                    f'cannot construct instance of {type(self).__name__}: '
                    f'invalid value {arg!r} for the {fieldname!r} field')

            setattr(self, fieldname, arg)

    @classmethod
    def parse(cls, buffer: binwrapper.BinWrapper) -> Struct:
        kwargs: typing.Dict[str, any] = {}
        for fieldname, field in cls._fields.items():
            if fieldname in {'mtype', 'message_length'}:
                continue
            kwargs[fieldname] = field.parse(buffer)
        return cls(**kwargs)

    @classmethod
    def dump(cls, val: Struct, buffer: binwrapper.BinWrapper) -> None:
        fields = val._fields
        for fieldname, field in fields.items():
            if fieldname in {'mtype', 'message_length'}:
                continue
            fval = getattr(val, fieldname)
            field.dump(fval, buffer)

    def __repr__(self):
        res = [f'<{type(self).__name__}']
        for fieldname in type(self)._fields:
            if fieldname in {'mtype', 'message_length'}:
                continue
            val = getattr(self, fieldname)
            res.append(f' {fieldname}={val!r}')
        res.append('>')
        return ''.join(res)

    @classmethod
    def render_field(
        cls, fieldname: str, buf: render_utils.RenderBuffer
    ) -> None:
        buf.write(f'{cls.__name__.ljust(_PAD - 1)} {fieldname};')

    @classmethod
    def render(cls) -> str:
        buf = render_utils.RenderBuffer()

        buf.write(f'struct {cls.__name__} {{')
        with buf.indent():
            for fieldname, field in cls._fields.items():
                if field.doc:
                    buf.write_comment(field.doc)
                field.render_field(fieldname, buf)
                buf.newline()

        if buf.lastline() == '':
            buf.popline()

        buf.write('};')
        return str(buf)


class KeyValue(Struct):
    code = UInt16('Key code (specific to the type of the Message).')
    value = Bytes('Value data.')


class Annotation(Struct):
    name = String('Name of the annotation')
    value = String('Value of the annotation (in JSON format).')


KeyValues = ArrayOf(UInt16, KeyValue, 'A set of key-value pairs.')
Annotations = ArrayOf(UInt16, Annotation, 'A set of annotations.')
MessageLength = UInt32('Length of message contents in bytes, including self.')

MessageType = (lambda letter: UInt8(f"Message type ('{letter}').",
                                    default=ord(letter)))


class Message(Struct, abstract=True):
    pass


class ServerMessage(Message, abstract=True):

    index: typing.Dict[int, typing.List[typing.Type[ServerMessage]]] = {}

    def __init_subclass__(cls):
        super().__init_subclass__()

        if 'mtype' not in cls._fields:
            raise TypeError(f'mtype field is missing for {cls}')
        if 'message_length' not in cls._fields:
            raise TypeError(f'message_length field is missing for {cls}')

        cls.index.setdefault(cls._fields['mtype'].default, []).append(cls)

    @classmethod
    def parse(cls, mtype: int, data: bytes) -> ServerMessage:
        iobuf = io.BytesIO(data)
        buffer = binwrapper.BinWrapper(iobuf)

        kwargs: typing.Dict[str, any] = {}

        msg_types = cls.index.get(mtype)
        if not msg_types:
            raise ValueError(f"unspecced message type {chr(mtype)!r}")
        if len(msg_types) > 1:
            raise ValueError(f"multiple specs for message type {chr(mtype)!r}")
        msg_type = msg_types[0]

        for fieldname, field in msg_type._fields.items():
            if fieldname in {'mtype', 'message_length'}:
                continue
            kwargs[fieldname] = field.parse(buffer)

        if len(iobuf.read(1)):
            raise ValueError(
                f'buffer is not empty after parsing {chr(mtype)!r} message')

        return msg_type(**kwargs)


class ClientMessage(Message, abstract=True):

    def __init_subclass__(cls):
        super().__init_subclass__()

        if 'mtype' not in cls._fields:
            raise TypeError(f'mtype field is missing for {cls}')
        if 'message_length' not in cls._fields:
            raise TypeError(f'message_length field is missing for {cls}')

    def dump(self) -> bytes:
        iobuf = io.BytesIO()
        buf = binwrapper.BinWrapper(iobuf)
        fields = type(self)._fields
        for fieldname, field in fields.items():
            if fieldname in {'mtype', 'message_length'}:
                continue
            val = getattr(self, fieldname)
            field.dump(val, buf)

        dumped = iobuf.getvalue()
        return (
            fields['mtype'].default.to_bytes(1, 'big') +
            (len(dumped) + 4).to_bytes(4, 'big') +
            dumped
        )


###############################################################################
# Protocol Messages Definitions
###############################################################################


class InputLanguage(enum.Enum):

    EDGEQL = 0x45  # b'E'
    SQL = 0x53  # b'S'


class OutputFormat(enum.Enum):

    BINARY = 0x62
    JSON = 0x6a
    JSON_ELEMENTS = 0x4a
    NONE = 0x6e


class Capability(enum.IntFlag):

    MODIFICATIONS     = 1 << 0    # noqa
    SESSION_CONFIG    = 1 << 1    # noqa
    TRANSACTION       = 1 << 2    # noqa
    DDL               = 1 << 3    # noqa
    PERSISTENT_CONFIG = 1 << 4    # noqa
    ALL               = 0xFFFFFFFFFFFFFFFF  # noqa


class CompilationFlag(enum.IntFlag):

    INJECT_OUTPUT_TYPE_IDS   = 1 << 0    # noqa
    INJECT_OUTPUT_TYPE_NAMES = 1 << 1    # noqa
    INJECT_OUTPUT_OBJECT_IDS = 1 << 2    # noqa


class DumpFlag(enum.IntFlag):

    DUMP_SECRETS = 1 << 0    # noqa


class ErrorSeverity(enum.Enum):
    ERROR = 120
    FATAL = 200
    PANIC = 255


class ErrorResponse(ServerMessage):

    mtype = MessageType('E')
    message_length = MessageLength
    severity = EnumOf(UInt8, ErrorSeverity, 'Message severity.')
    error_code = UInt32('Message code.')
    message = String('Error message.')
    attributes = ArrayOf(UInt16, KeyValue, 'Error attributes.')


class MessageSeverity(enum.Enum):
    DEBUG = 20
    INFO = 40
    NOTICE = 60
    WARNING = 80


class LogMessage(ServerMessage):

    mtype = MessageType('L')
    message_length = MessageLength
    severity = EnumOf(UInt8, MessageSeverity, 'Message severity.')
    code = UInt32('Message code.')
    text = String('Message text.')
    annotations = ArrayOf(UInt16, Annotation, 'Message annotations.')


class TransactionState(enum.Enum):

    NOT_IN_TRANSACTION = 0x49
    IN_TRANSACTION = 0x54
    IN_FAILED_TRANSACTION = 0x45


class ReadyForCommand(ServerMessage):

    mtype = MessageType('Z')
    message_length = MessageLength
    annotations = Annotations
    transaction_state = EnumOf(UInt8, TransactionState, 'Transaction state.')


class RestoreReady(ServerMessage):

    mtype = MessageType('+')
    message_length = MessageLength
    annotations = Annotations
    jobs = UInt16('Number of parallel jobs for restore, currently always "1"')


class DataElement(Struct):

    data = ArrayOf(UInt32, UInt8(), 'Encoded output data.')


class CommandComplete(ServerMessage):

    mtype = MessageType('C')
    message_length = MessageLength
    annotations = Annotations
    capabilities = EnumOf(UInt64, Capability,
                          'A bit mask of allowed capabilities.')
    status = String('Command status.')

    state_typedesc_id = UUID('State data descriptor ID.')
    state_data = Bytes('Encoded state data.')


class CommandDataDescription(ServerMessage):

    mtype = MessageType('T')
    message_length = MessageLength
    annotations = Annotations
    capabilities = EnumOf(UInt64, Capability,
                          'A bit mask of allowed capabilities.')
    result_cardinality = EnumOf(
        UInt8, Cardinality, 'Actual result cardinality.')
    input_typedesc_id = UUID('Argument data descriptor ID.')
    input_typedesc = Bytes('Argument data descriptor.')
    output_typedesc_id = UUID('Output data descriptor ID.')
    output_typedesc = Bytes('Output data descriptor.')


class StateDataDescription(ServerMessage):

    mtype = MessageType('s')
    message_length = MessageLength
    typedesc_id = UUID('Updated state data descriptor ID.')
    typedesc = Bytes('State data descriptor.')


class Data(ServerMessage):

    mtype = MessageType('D')
    message_length = MessageLength

    data = ArrayOf(
        UInt16,
        DataElement,
        'Encoded output data array. The array is currently always of size 1.'
    )


class DumpTypeInfo(Struct):

    type_name = String()
    type_class = String()
    type_id = UUID()


class DumpObjectDesc(Struct):

    object_id = UUID()
    description = Bytes()
    dependencies = ArrayOf(UInt16, UUID())


class DumpHeader(ServerMessage):

    mtype = MessageType('@')
    message_length = MessageLength
    attributes = KeyValues
    major_ver = UInt16('Major version of Gel.')
    minor_ver = UInt16('Minor version of Gel.')
    schema_ddl = String('Schema.')
    types = ArrayOf(UInt32, DumpTypeInfo, 'Type identifiers.')
    descriptors = ArrayOf(UInt32, DumpObjectDesc, 'Object descriptors.')


class DumpBlock(ServerMessage):

    mtype = MessageType('=')
    message_length = MessageLength
    attributes = KeyValues


class ServerKeyData(ServerMessage):

    mtype = MessageType('K')
    message_length = MessageLength
    data = FixedArrayOf(32, UInt8(), 'Key data.')


class ParameterStatus(ServerMessage):

    mtype = MessageType('S')
    message_length = MessageLength
    name = Bytes('Parameter name.')
    value = Bytes('Parameter value.')


class ParameterStatus_SystemConfig(Struct):

    typedesc = ArrayOf(UInt32, UInt8(), 'Type descriptor prefixed with '
                                        'type descriptor uuid.')
    data = FixedArrayOf(1, DataElement, 'Configuration settings data.')


class ProtocolExtension(Struct):

    name = String('Extension name.')
    annotations = ArrayOf(UInt16, Annotation, 'A set of extension annotaions.')


class ServerHandshake(ServerMessage):

    mtype = MessageType('v')
    message_length = MessageLength
    major_ver = UInt16('maximum supported or client-requested '
                       'protocol major version, whichever is greater.')
    minor_ver = UInt16('maximum supported or client-requested '
                       'protocol minor version, whichever is greater.')
    extensions = ArrayOf(
        UInt16, ProtocolExtension, 'Supported protocol extensions.')


class AuthenticationOK(ServerMessage):

    mtype = MessageType('R')
    message_length = MessageLength
    auth_status = UInt32('Specifies that this message contains '
                         'a successful authentication indicator.',
                         default=0x0)


class AuthenticationRequiredSASLMessage(ServerMessage):

    mtype = MessageType('R')
    message_length = MessageLength
    auth_status = UInt32('Specifies that this message contains '
                         'a SASL authentication request.',
                         default=0x0A)
    methods = ArrayOf(UInt32, String(),
                      'A list of supported SASL authentication methods.')


class AuthenticationSASLContinue(ServerMessage):

    mtype = MessageType('R')
    message_length = MessageLength
    auth_status = UInt32('Specifies that this message contains '
                         'a SASL challenge.',
                         default=0x0B)
    sasl_data = Bytes('Mechanism-specific SASL data.')


class AuthenticationSASLFinal(ServerMessage):

    mtype = MessageType('R')
    message_length = MessageLength
    auth_status = UInt32('Specifies that SASL authentication '
                         'has completed.',
                         default=0x0C)
    sasl_data = Bytes()


class Dump(ClientMessage):

    mtype = MessageType('>')
    message_length = MessageLength
    annotations = Annotations
    flags = EnumOf(UInt64, DumpFlag, 'A bit mask of dump options.')


class Sync(ClientMessage):

    mtype = MessageType('S')
    message_length = MessageLength


class Flush(ClientMessage):

    mtype = MessageType('H')
    message_length = MessageLength


class Restore(ClientMessage):

    mtype = MessageType('<')
    message_length = MessageLength
    attributes = KeyValues
    jobs = UInt16(
        'Number of parallel jobs for restore (only "1" is supported)')
    header_data = Bytes(
        'Original DumpHeader packet data excluding mtype and message_length')


class RestoreBlock(ClientMessage):

    mtype = MessageType('=')
    message_length = MessageLength
    block_data = Bytes(
        'Original DumpBlock packet data excluding mtype and message_length')


class RestoreEof(ClientMessage):

    mtype = MessageType('.')
    message_length = MessageLength


class Parse(ClientMessage):

    mtype = MessageType('P')
    message_length = MessageLength
    annotations = Annotations
    allowed_capabilities = EnumOf(UInt64, Capability,
                                  'A bit mask of allowed capabilities.')
    compilation_flags = EnumOf(UInt64, CompilationFlag,
                               'A bit mask of query options.')
    implicit_limit = UInt64('Implicit LIMIT clause on returned sets.')
    input_language = EnumOf(UInt8, InputLanguage, 'Command source language.')
    output_format = EnumOf(UInt8, OutputFormat, 'Data output format.')
    expected_cardinality = EnumOf(UInt8, Cardinality,
                                  'Expected result cardinality.')
    command_text = String('Command text.')
    state_typedesc_id = UUID('State data descriptor ID.')
    state_data = Bytes('Encoded state data.')


class Execute(ClientMessage):

    mtype = MessageType('O')
    message_length = MessageLength
    annotations = Annotations
    allowed_capabilities = EnumOf(UInt64, Capability,
                                  'A bit mask of allowed capabilities.')
    compilation_flags = EnumOf(UInt64, CompilationFlag,
                               'A bit mask of query options.')
    implicit_limit = UInt64('Implicit LIMIT clause on returned sets.')
    input_language = EnumOf(UInt8, InputLanguage, 'Command source language.')
    output_format = EnumOf(UInt8, OutputFormat, 'Data output format.')
    expected_cardinality = EnumOf(UInt8, Cardinality,
                                  'Expected result cardinality.')
    command_text = String('Command text.')
    state_typedesc_id = UUID('State data descriptor ID.')
    state_data = Bytes('Encoded state data.')

    input_typedesc_id = UUID('Argument data descriptor ID.')
    output_typedesc_id = UUID('Output data descriptor ID.')
    arguments = Bytes('Encoded argument data.')


class ConnectionParam(Struct):

    name = String()
    value = String()


class ClientHandshake(ClientMessage):

    mtype = MessageType('V')
    message_length = MessageLength
    major_ver = UInt16('Requested protocol major version.')
    minor_ver = UInt16('Requested protocol minor version.')
    params = ArrayOf(UInt16, ConnectionParam, 'Connection parameters.')
    extensions = ArrayOf(
        UInt16, ProtocolExtension, 'Requested protocol extensions.')


class Terminate(ClientMessage):

    mtype = MessageType('X')
    message_length = MessageLength


class AuthenticationSASLInitialResponse(ClientMessage):

    mtype = MessageType('p')
    message_length = MessageLength
    method = String('Name of the SASL authentication mechanism '
                    'that the client selected.')
    sasl_data = Bytes('Mechanism-specific "Initial Response" data.')


class AuthenticationSASLResponse(ClientMessage):

    mtype = MessageType('r')
    message_length = MessageLength
    sasl_data = Bytes('Mechanism-specific response data.')
