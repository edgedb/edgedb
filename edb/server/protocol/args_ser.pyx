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
from edb.server.compiler import enums
from edb.server.dbview cimport dbview

from edb.server.pgproto cimport hton
from edb.server.pgproto.pgproto cimport (
    WriteBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_get_len,
    frb_slice_from,
)

cdef uint32_t SCALAR_TAG = int(enums.TypeTag.SCALAR)
cdef uint32_t TUPLE_TAG = int(enums.TypeTag.TUPLE)
cdef uint32_t ARRAY_TAG = int(enums.TypeTag.ARRAY)


cdef recode_bind_args_for_script(
    dbview.DatabaseConnectionView dbv,
    dbview.CompiledQuery compiled,
    bytes bind_args,
    ssize_t start,
    ssize_t end,
):
    cdef:
        WriteBuffer bind_data
        ssize_t i
        ssize_t oidx
        ssize_t iidx

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

        num_args = query_unit.in_type_args_real_count
        num_args += _count_globals(query_unit)

        if compiled.first_extra is not None:
            num_args += compiled.extra_counts[i]

        bind_data.write_int16(<int16_t>num_args)

        if query_unit.in_type_args:
            for iidx, arg in enumerate(query_unit.in_type_args):
                oidx = arg.outer_idx if arg.outer_idx is not None else iidx
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
    list data_types = None,
):
    cdef:
        FRBuffer in_buf
        FRBuffer peek_buf
        WriteBuffer out_buf = WriteBuffer.new()
        int32_t recv_args
        int32_t decl_args
        ssize_t in_len
        ssize_t i
        int32_t array_tid
        const char *data
        bint live = positions is None

    assert cpython.PyBytes_CheckExact(bind_args)
    frb_init(
        &in_buf,
        cpython.PyBytes_AS_STRING(bind_args),
        cpython.Py_SIZE(bind_args))

    # number of elements in the tuple
    # for empty tuple it's okay to send zero-length arguments
    qug = compiled.query_unit_group
    is_null_type = qug.in_type_id == sertypes.NULL_TYPE_ID.bytes
    if frb_get_len(&in_buf) == 0:
        if not is_null_type:
            raise errors.InputDataError(
                f"insufficient data for type-id {qug.in_type_id}")
        recv_args = 0
    else:
        if is_null_type:
            raise errors.InputDataError(
                "absence of query arguments must be encoded with a "
                "'zero' type "
                "(id: 00000000-0000-0000-0000-000000000000, "
                "encoded with zero bytes)")
        recv_args = hton.unpack_int32(frb_read(&in_buf, 4))
    decl_args = len(qug.in_type_args or ())

    if recv_args != decl_args:
        raise errors.InputDataError(
            f"invalid argument count, "
            f"expected: {decl_args}, got: {recv_args}")

    num_args = qug.in_type_args_real_count
    if compiled.first_extra is not None:
        assert recv_args == compiled.first_extra, \
            f"argument count mismatch {recv_args} != {compiled.first_extra}"
        num_args += compiled.extra_counts[0]

    num_globals = _count_globals(qug)
    num_args += num_globals

    if live:
        if not compiled.extra_formatted_as_text:
            # all parameter values are in binary
            out_buf.write_int32(0x00010001)
        elif not recv_args and not num_globals:
            # all parameter values are in text (i.e extracted SQL constants)
            out_buf.write_int16(0x0000)
        else:
            # got a mix of binary and text, spell them out explicitly
            out_buf.write_int16(<int16_t>num_args)
            # explicit args are in binary
            for _ in range(recv_args):
                out_buf.write_int16(0x0001)
            # and extracted SQL constants are in text
            if compiled.extra_counts:
                for _ in range(compiled.extra_counts[0]):
                    out_buf.write_int16(0x0000)
            # and injected globals are binary again
            for _ in range(num_globals):
                out_buf.write_int16(0x0001)

        out_buf.write_int16(<int16_t>num_args)

    if data_types is not None and compiled.extra_type_oids:
        data_types.extend([0] * recv_args)
        data_types.extend(compiled.extra_type_oids)
        data_types.extend([0] * num_globals)

    if qug.in_type_args:
        for param in qug.in_type_args:
            if positions is not None:
                positions.append(out_buf._length)

            frb_read(&in_buf, 4)  # reserved
            # Some of the logic paths below need the length are cleaner if
            # the length is still present in the input buf, so we just
            # *peek* at the length here, and need to consume it later.
            peek_buf = in_buf
            in_len = hton.unpack_int32(frb_read(&peek_buf, 4))
            if in_len < 0:
                # This means argument value is NULL
                if param.required:
                    raise errors.QueryError(
                        f"parameter ${param.name} is required")

            # If the param has encoded tuples, we need to decode them
            # and reencode them as arrays of scalars.
            if param.sub_params:
                tids, trans_typ = param.sub_params
                _decode_tuple_args(
                    dbv, &in_buf, out_buf, in_len, tids, trans_typ)
                continue

            frb_read(&in_buf, 4)
            out_buf.write_int32(in_len)

            if in_len > 0:
                if param.array_type_id is not None:
                    array_tid = dbv.resolve_backend_type_id(
                        param.array_type_id)
                    recode_array(dbv, &in_buf, out_buf, in_len, array_tid, None)
                else:
                    data = frb_read(&in_buf, in_len)
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

    if frb_get_len(&in_buf):
        raise errors.InputDataError('unexpected trailing data in buffer')

    return out_buf


cdef bytes recode_global(
    dbv: dbview.DatabaseConnectionView,
    glob: bytes,
    glob_descriptor: object,
):
    cdef:
        WriteBuffer out_buf
        FRBuffer in_buf

    if glob_descriptor is None:
        return glob

    out_buf = WriteBuffer.new()

    assert cpython.PyBytes_CheckExact(glob)
    frb_init(
        &in_buf,
        cpython.PyBytes_AS_STRING(glob),
        cpython.Py_SIZE(glob))

    _recode_global(dbv, &in_buf, out_buf, in_buf.len, glob_descriptor)

    if frb_get_len(&in_buf):
        raise errors.InputDataError('unexpected trailing data in buffer')

    return bytes(memoryview(out_buf))


cdef _recode_global(
    dbv: dbview.DatabaseConnectionView,
    FRBuffer* in_buf,
    out_buf: WriteBuffer,
    in_len: ssize_t,
    glob_descriptor: object,
):
    if glob_descriptor is None:
        data = frb_read(in_buf, in_len)
        out_buf.write_cstr(data, in_len)
    elif glob_descriptor[0] == TUPLE_TAG:
        _, el_tids, el_infos = glob_descriptor
        recode_global_tuple(dbv, in_buf, out_buf, in_len, el_tids, el_infos)
    elif glob_descriptor[0] == ARRAY_TAG:
        _, el_tid, tuple_info = glob_descriptor
        btid = dbv.resolve_backend_type_id(el_tid)
        recode_array(dbv, in_buf, out_buf, in_len, btid, tuple_info)


cdef recode_global_tuple(
    dbv: dbview.DatabaseConnectionView,
    FRBuffer* in_buf,
    out_buf: WriteBuffer,
    in_len: ssize_t,
    el_tids: tuple,
    el_infos: tuple,
):
    """
    Tuples in globals need to have NULLs checked and oids injected,
    like arrays do.

    Annoyingly this is a *totally separate* code path than tuple query
    parameters go through. This is because global tuples actually can
    get passed as postgres composite types, since they are declared in
    the schema.
    """
    cdef:
        WriteBuffer buf
        ssize_t cnt
        ssize_t idx
        ssize_t num
        ssize_t tag
        FRBuffer sub_buf

    frb_slice_from(&sub_buf, in_buf, in_len)

    cnt = <uint32_t>hton.unpack_int32(frb_read(&sub_buf, 4))
    out_buf.write_int32(cnt)
    num = len(el_tids)
    if cnt != num:
        raise errors.InputDataError(
            f"tuple length mismatch: {cnt} vs {num}")
    for idx in range(num):
        frb_read(&sub_buf, 4)
        el_btid = dbv.resolve_backend_type_id(el_tids[idx])
        out_buf.write_int32(<int32_t>el_btid)

        in_len = hton.unpack_int32(frb_read(&sub_buf, 4))
        if in_len < 0:
            raise errors.InputDataError("invalid NULL inside type")
        out_buf.write_int32(in_len)
        _recode_global(dbv, &sub_buf, out_buf, in_len, el_infos[idx])

    if frb_get_len(&sub_buf):
        raise errors.InputDataError('unexpected trailing data in buffer')


cdef recode_array(
    dbv: dbview.DatabaseConnectionView,
    FRBuffer* in_buf,
    out_buf: WriteBuffer,
    in_len: ssize_t,
    array_tid: int32_t,
    tuple_info: object,
):
    # For a standalone array, we still need to inject oids and reject
    # NULL elements.
    cdef:
        ssize_t cnt
        ssize_t idx
        ssize_t num
        ssize_t tag
        FRBuffer sub_buf

    frb_slice_from(&sub_buf, in_buf, in_len)

    ndims = hton.unpack_int32(frb_read(&sub_buf, 4)) # ndims
    if ndims != 1 and ndims != 0:
        raise errors.InputDataError("unsupported array dimensions")
    out_buf.write_int32(ndims)

    data = frb_read(&sub_buf, 8)  # flags + reserved (oid)
    out_buf.write_cstr(data, 4)  # just write flags
    out_buf.write_int32(<int32_t>array_tid)

    if ndims != 0:
        cnt = hton.unpack_int32(frb_read(&sub_buf, 4))
        out_buf.write_int32(cnt)

        val = hton.unpack_int32(frb_read(&sub_buf, 4)) # bound
        if val != 1:
            raise errors.InputDataError("unsupported array bound")
        out_buf.write_int32(val)

        # We have to actually scan the array to make sure it
        # doesn't have any NULLs in it.
        for idx in range(cnt):
            in_len = hton.unpack_int32(frb_read(&sub_buf, 4))
            if in_len < 0:
                raise errors.InputDataError("invalid NULL inside type")
            out_buf.write_int32(in_len)
            if tuple_info is None:
                data = frb_read(&sub_buf, in_len)
                out_buf.write_cstr(data, in_len)
            else:
                _recode_global(dbv, &sub_buf, out_buf, in_len, tuple_info)
        if frb_get_len(&sub_buf):
            raise errors.InputDataError('unexpected trailing data in buffer')


cdef _decode_tuple_args_core(
    FRBuffer* in_buf,
    out_bufs: tuple[WriteBuffer],
    counts: list[int],
    acounts: list[int],
    trans_typ: tuple,
    in_array: bool,
):
    # Recurse over the types and the input data, collecting the
    # arguments into the various out_bufs. See
    # edb.edgeql.compiler.tuple_args for more discussion.

    cdef:
        ssize_t in_len
        WriteBuffer buf
        ssize_t cnt
        ssize_t idx
        ssize_t num
        ssize_t tag
        int32_t val
        FRBuffer sub_buf

    tag = trans_typ[0]
    idx = trans_typ[1]

    in_len = hton.unpack_int32(frb_read(in_buf, 4))
    buf = out_bufs[idx]

    if in_len < 0:
        raise errors.InputDataError("invalid NULL inside type")

    frb_slice_from(&sub_buf, in_buf, in_len)

    if tag == SCALAR_TAG:
        buf.write_int32(in_len)
        data = frb_read(&sub_buf, in_len)
        buf.write_cstr(data, in_len)
        if in_array:
            counts[idx] += 1

    elif tag == TUPLE_TAG:
        cnt = <uint32_t>hton.unpack_int32(frb_read(&sub_buf, 4))
        num = len(trans_typ) - 2
        if cnt != num:
            raise errors.InputDataError(
                f"tuple length mismatch: {cnt} vs {num}")
        for idx in range(num):
            typ = trans_typ[idx + 2]
            frb_read(&sub_buf, 4)
            _decode_tuple_args_core(
                &sub_buf, out_bufs, counts, acounts, typ, in_array)

    elif tag == ARRAY_TAG:
        val = hton.unpack_int32(frb_read(&sub_buf, 4)) # ndims
        if val != 1 and val != 0:
            raise errors.InputDataError("unsupported array dimensions")
        frb_read(&sub_buf, 4)  # flags
        frb_read(&sub_buf, 4)  # reserved
        cnt = <uint32_t>hton.unpack_int32(frb_read(&sub_buf, 4))
        val = hton.unpack_int32(frb_read(&sub_buf, 4)) # bound
        if val != 1:
            raise errors.InputDataError("unsupported array bound")

        # For nested arrays, we need to produce an array containing
        # the start/end indexes in the flattened array.
        if in_array:
            # If this is the first element, put in the 0
            if acounts[idx] == -1:
                counts[idx] += 1
                acounts[idx] = 0
                buf.write_int32(4)
                buf.write_int32(0)
            counts[idx] += 1
            acounts[idx] += cnt
            buf.write_int32(4)
            buf.write_int32(acounts[idx])

        styp = trans_typ[2]
        for _ in range(cnt):
            _decode_tuple_args_core(
                &sub_buf, out_bufs, counts, acounts, styp, True)

    if frb_get_len(&sub_buf):
        raise errors.InputDataError('unexpected trailing data in buffer')


cdef WriteBuffer _decode_tuple_args(
    dbv: dbview.DatabaseConnectionView,
    FRBuffer* in_buf,
    out_buf: WriteBuffer,
    in_len: ssize_t,
    tids: list,
    trans_typ: object,
):
    # PERF: Can we use real arrays, instead of python lists?
    cdef:
        const char *data
        list buffers
        list counts
        list acounts
        WriteBuffer buf

    # N.B: We have peeked at in_len, but the size is still in the buffer, for
    # more convenient processing by _decode_tuple_args_core

    if in_len < 0:
        # For a NULL argument, fill out *every* one of our args with NULL
        for _ in tids:
            out_buf.write_int32(in_len)
        # We only peeked at in_len before, so consume it now
        frb_read(in_buf, 4)
        return

    buffers = []
    counts = []
    acounts = []
    for maybe_tid in tids:
        buf = WriteBuffer.new()
        counts.append(0 if maybe_tid else -1)
        acounts.append(-1)
        buffers.append(buf)

    _decode_tuple_args_core(
        in_buf, tuple(buffers), counts, acounts, trans_typ, False)

    # zip all of the buffers we have collected into up
    # PERF: or should we just index?
    for maybe_tid, count, buf in zip(tids, counts, buffers):
        if maybe_tid:
            ndims = 1
            out_buf.write_int32(12 + 8 * ndims + buf.len())
            # ndimensions + flags
            array_tid = dbv.resolve_backend_type_id(maybe_tid)
            out_buf.write_int32(1)
            out_buf.write_int32(0)
            out_buf.write_int32(<int32_t>array_tid)

            out_buf.write_int32(<int32_t>count)
            out_buf.write_int32(1)

        out_buf.write_buffer(buf)


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
        if val is not None:
            out_buf.write_int32(len(val))
            out_buf.write_bytes(val)
        else:
            out_buf.write_int32(-1)
        if has_present_arg:
            out_buf.write_int32(1)
            present = b'\x01' if entry is not None else b'\x00'
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


cdef WriteBuffer combine_raw_args(
    args: tuple[bytes, ...] | list[bytes] = (),
):
    cdef:
        int arg_len
        WriteBuffer bind_data = WriteBuffer.new()

    if len(args) > 32767:
        raise AssertionError(
            'the number of query arguments cannot exceed 32767')

    bind_data.write_int32(0x00010001)
    bind_data.write_int16(<int16_t> len(args))
    for arg in args:
        if arg is None:
            bind_data.write_int32(-1)
        else:
            arg_len = len(arg)
            if arg_len > 0x7fffffff:
                raise ValueError("argument too long")
            bind_data.write_int32(<int32_t> arg_len)
            bind_data.write_bytes(arg)
    bind_data.write_int32(0x00010001)

    return bind_data
