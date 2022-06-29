#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

cimport cpython

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t

from edb import errors
from edb.server.compiler import sertypes
from edb.server.dbview cimport dbview

from edb.server.pgproto cimport hton
from edb.server.pgproto.pgproto cimport (
    WriteBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_get_len,
)


cdef recode_bind_args_for_script(
    dbview.DatabaseConnectionView dbv,
    dbview.CompiledQuery compiled,
    bytes bind_args,
    ssize_t start,
    ssize_t end,
):
    cdef:
        WriteBuffer bind_data

    unit_group = compiled.query_unit_group

    # TODO: just do the simple thing if it is only one!

    positions = []
    recoded_buf = recode_bind_args(dbv, compiled, bind_args, positions)
    # TODO: something with less copies
    recoded = bytes(memoryview(recoded_buf))

    bind_array = []
    for i in range(start, end):
        query_unit = unit_group[i]
        bind_data = WriteBuffer.new()
        bind_data.write_int32(0x00010001)

        num_args = len(query_unit.in_type_args or ())
        num_args += _count_globals(query_unit)

        if compiled.first_extra is not None:
            num_args += compiled.extra_counts[i]

        bind_data.write_int16(<int16_t>num_args)

        if query_unit.in_type_args:
            for arg in query_unit.in_type_args:
                oidx = arg.outer_idx
                barg = recoded[positions[oidx]:positions[oidx+1]]
                bind_data.write_bytes(barg)

        if compiled.first_extra is not None:
            bind_data.write_bytes(compiled.extra_blobs[i])

        _inject_globals(dbv, query_unit, bind_data)

        bind_data.write_int32(0x00010001)

        bind_array.append(bind_data)

    return bind_array


cdef WriteBuffer recode_bind_args(
    dbview.DatabaseConnectionView dbv,
    dbview.CompiledQuery compiled,
    bytes bind_args,
    # XXX do something better?!?
    list positions = None,
):
    cdef:
        FRBuffer in_buf
        WriteBuffer out_buf = WriteBuffer.new()
        int32_t recv_args
        int32_t decl_args
        ssize_t in_len
        ssize_t i
        const char *data
        bint live = positions is None

    assert cpython.PyBytes_CheckExact(bind_args)
    frb_init(
        &in_buf,
        cpython.PyBytes_AS_STRING(bind_args),
        cpython.Py_SIZE(bind_args))

    # all parameters are in binary
    if live:
        out_buf.write_int32(0x00010001)

    # number of elements in the tuple
    # for empty tuple it's okay to send zero-length arguments
    qug = compiled.query_unit_group
    is_null_type = qug.in_type_id == sertypes.NULL_TYPE_ID.bytes
    if frb_get_len(&in_buf) == 0:
        if not is_null_type:
            raise errors.ProtocolError(
                f"insufficient data for type-id {qug.in_type_id}")
        recv_args = 0
    else:
        if is_null_type:
            raise errors.ProtocolError(
                "absence of query arguments must be encoded with a "
                "'zero' type "
                "(id: 00000000-0000-0000-0000-000000000000, "
                "encoded with zero bytes)")
        recv_args = hton.unpack_int32(frb_read(&in_buf, 4))
    decl_args = len(qug.in_type_args or ())

    if recv_args != decl_args:
        raise errors.QueryError(
            f"invalid argument count, "
            f"expected: {decl_args}, got: {recv_args}")

    num_args = recv_args
    if compiled.first_extra is not None:
        assert recv_args == compiled.first_extra, \
            f"argument count mismatch {recv_args} != {compiled.first_extra}"
        num_args += compiled.extra_counts[0]

    num_args += _count_globals(qug)

    if live:
        out_buf.write_int16(<int16_t>num_args)

    if qug.in_type_args:
        for param in qug.in_type_args:
            if positions is not None:
                positions.append(out_buf._length)

            frb_read(&in_buf, 4)  # reserved
            in_len = hton.unpack_int32(frb_read(&in_buf, 4))
            out_buf.write_int32(in_len)

            if in_len < 0:
                # This means argument value is NULL
                if param.required:
                    raise errors.QueryError(
                        f"parameter ${param.name} is required")

            if in_len > 0:
                data = frb_read(&in_buf, in_len)
                # Ensure all array parameters have correct element OIDs as
                # per Postgres' expectations.
                if param.array_type_id is not None:
                    # ndimensions + flags
                    array_tid = dbv.resolve_backend_type_id(
                        param.array_type_id)
                    out_buf.write_cstr(data, 8)
                    out_buf.write_int32(<int32_t>array_tid)
                    out_buf.write_cstr(&data[12], in_len - 12)
                else:
                    out_buf.write_cstr(data, in_len)

    if positions is not None:
        positions.append(out_buf._length)

    if live:
        if compiled.first_extra is not None:
            out_buf.write_bytes(compiled.extra_blobs[0])

        # Inject any globals variables into the argument stream.
        _inject_globals(dbv, qug, out_buf)

        # All columns are in binary format
        out_buf.write_int32(0x00010001)

    return out_buf


cdef _inject_globals(
    dbv: dbview.DatabaseConnectionView,
    query_unit_or_group: object,
    out_buf: WriteBuffer,
):
    globals = query_unit_or_group.globals
    if not globals:
        return

    state_globals = dbv.get_globals()
    for (name, has_present_arg) in globals:
        val = None
        entry = state_globals.get(name)
        if entry:
            val = entry.value
        if val:
            out_buf.write_int32(len(val))
            out_buf.write_bytes(val)
        else:
            out_buf.write_int32(-1)
        if has_present_arg:
            out_buf.write_int32(1)
            present = b'\x01' if val is not None else b'\x00'
            out_buf.write_bytes(present)


cdef uint64_t _count_globals(
    query_unit: object,
):
    cdef:
        uint64_t num_args

    num_args = 0
    if query_unit.globals:
        num_args += len(query_unit.globals)
        for _, has_present_arg in query_unit.globals:
            if has_present_arg:
                num_args += 1

    return num_args
