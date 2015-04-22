##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""This package provides utilities to allow creation and modification of
python code objects at the opcode level.

Opcodes are defined in the :py:mod:`metamagic.utils.lang.python.code.opcodes` module.
Some information about opcodes themselves can be found in :py:mod:`dis` module
documentation in the standard library."""


import dis
import types
import copy

from metamagic.utils.datastructures import OrderedSet
from . import opcodes


#: Used to order cellvars & freevars inthe  'to_code' method
#:
_MAX_CELL_WEIGHT = 1000000


OP_SETUP_EXCEPT     = opcodes.SETUP_EXCEPT
OP_SETUP_FINALLY    = opcodes.SETUP_FINALLY
OP_JUMP_ABSOLUTE    = opcodes.JUMP_ABSOLUTE
OP_JUMP_FORWARD     = opcodes.JUMP_FORWARD
OP_FOR_ITER         = opcodes.FOR_ITER
OP_EXTENDED_ARG     = opcodes.EXTENDED_ARG
OP_YIELD_VALUE      = opcodes.YIELD_VALUE
OP_YIELD_FROM       = getattr(opcodes, 'YIELD_FROM', None)
OP_EXTENDED_ARG     = opcodes.EXTENDED_ARG
OP_RETURN_VALUE     = opcodes.RETURN_VALUE

#: If we have any of the following opcodes is in the code object -
#: its CO_OPTIMIZED flag should be off.
#:
_OPTIMIZE_NEG_SET   = {opcodes.LOAD_NAME, opcodes.STORE_NAME, opcodes.DELETE_NAME}


# Some important flags from 'Python/code.h'.
# Only actual and in-use flags are listed here.
#
CO_OPTIMIZED        = 0x0001
CO_NEWLOCALS        = 0x0002
CO_VARARGS          = 0x0004
CO_VARKEYWORDS      = 0x0008
CO_GENERATOR        = 0x0020
CO_NOFREE           = 0x0040


#: ``co_flags`` we support.
#: The constant is for the testing purposes.
#:
_SUPPORTED_FLAGS    = CO_OPTIMIZED | CO_NEWLOCALS | CO_VARARGS | CO_VARKEYWORDS | \
                      CO_GENERATOR | CO_NOFREE


class Code:
    '''A more convenient to modify representation of python code block.

    To simplify working with opcodes and their arguments, we treat opcodes as
    objects (see ``utils.lang.python.code.opcodes`` module for details), which
    encapsulate their arguments and their meaning.  For instance, the ``LOAD_FAST``
    opcode has attribute 'local', which contains the name of a local variable.

    As a usage example, imagine we need to transform a regular python function
    into a generator.

    .. code-block:: python

        def pow2(a):
            return a ** 2

    The above code produces the following bytecode (``dis.dis(pow2)``)::

        2           0 LOAD_FAST                0 (a)
                    3 LOAD_CONST               1 (2)
                    6 BINARY_POWER
                    7 RETURN_VALUE

    To transform ``pow2`` into a generator, we need to remove the ``RETURN_VALUE``
    opcode and insert ``YIELD_VALUE``, followed by ``RETURN_VALUE``, which should
    return ``None``:

    .. code-block:: python

        # Create an instance of `Code` object from standard
        # python code object
        #
        code = Code.from_code(pow2.__code__)

        # Delete the RETURN_VALUE opcode
        #
        del code.ops[-1]

        # Add opcodes necessary to define and correctly
        # terminate a generator function
        #
        code.ops.extend((opcodes.YIELD_VALUE(),
                         opcodes.POP_TOP(),
                         opcodes.LOAD_CONST(const=None),
                         opcodes.RETURN_VALUE()))

        # Replace the code object of ``pow2`` with the new,
        # augmented one
        #
        pow2.__code__ = code.to_code()

    Let's look at the updated ``pow2`` opcodes::

        2           0 LOAD_FAST                0 (a)
                    3 LOAD_CONST               1 (2)
                    6 BINARY_POWER
                    7 YIELD_VALUE
                    8 POP_TOP
                    9 LOAD_CONST               0 (None)
                   12 RETURN_VALUE

    The patched ``pow2`` is now a generator:

    .. code-block:: pycon

        >>> pow2(10)
        <generator object...

        >>> list(pow2(10))
        [100]
    '''

    __slots__ = 'ops', 'vararg', 'varkwarg', 'newlocals', 'filename', \
                'firstlineno', 'name', 'args', 'kwonlyargs', 'docstring', \
                'has_class_freevar'

    def __init__(self, ops=None, *, vararg=None, varkwarg=None, newlocals=False,
                 filename='<string>', firstlineno=0, name='<code>',
                 args=None, kwonlyargs=None, docstring=None, has_class_freevar=False):

        '''
        :param ops: List of ``opcode.OpCode`` instances.

        :param bool newlocals: Determines if a new local namespace should be created.
                               Should always be ``True`` for function code objects.

        :param string filename: The filename of the code object.
        :param int firstlineno: The starting line number of the first instruction of
                                the code object.
        :param string name: The name of the code object.  For function code objects it
                            is usually set to the name of the function.

        :param tuple(string) args: Function argument names.  Only valid for function code objects.

        :param tuple(string) kwonlyargs: Function keyword argument names.  Only valid for
                                         function code objects.

        :param string vararg: The name of the ``*args`` argument, i.e. for ``*arg``
                              it will be ``'arg'``.  Only valid for function code objects.

        :param string varkwarg: The name of the ``**kwargs`` argument, i.e. for ``**kwarg``
                                it will be ``'kwarg'``.  Only valid for function code objects.

        :param str docstring: Code object documenation string.

        :param bool has_class_freevar: Should be set to ``True``, if the code contains calls
                                       to ``super()``.  In this case ``Code.to_code()`` will
                                       generate one extra slot named ``__class__`` in
                                       ``co_freevars`` (see CPython's ``super()`` implementation
                                       in 'Objects/typeobject.c'.)

        .. note:: ``co_names``, ``co_consts``, ``co_varnames``, ``co_freevars``
                  and ``co_cellvars`` are computed dynamically from the opcodes in ``Code.ops``.
        '''

        if ops is None:
            self.ops = []
        else:
            self.ops = ops

        self.vararg = vararg
        self.varkwarg = varkwarg
        self.newlocals = newlocals
        self.filename = filename
        self.firstlineno = firstlineno
        self.name = name
        self.args = args
        self.kwonlyargs = kwonlyargs
        self.docstring = docstring
        self.has_class_freevar = has_class_freevar

    def _calc_stack_size(self):
        # The algorithm is almost directly translated to python from c,
        # from 'Python/compile.c', function 'stackdepth()'

        seen = set()
        startdepths = {}
        ops = self.ops
        ops_len = len(ops)

        idx = 0
        depth = 0
        maxdepth = 0
        idx_stack = []

        if ops_len <= 0:
            return 0

        while True:
            op = ops[idx]

            # we've come back in a loop OR we've already examined this path at a greater depth
            if idx in seen or startdepths.get(op, -100000) >= depth:
                # go back to the last idx jump
                try:
                    idx, depth, seen = idx_stack.pop()
                    continue
                except IndexError:
                    break

            seen.add(idx)
            startdepths[op] = depth

            # adjust stack depth based on the current op
            depth += op.stack_effect
            if depth > maxdepth:
                maxdepth = depth
            assert depth >= 0

            jump = None
            op_cls = op.__class__
            if op.has_jrel:
                jump = op.jrel
            elif op.has_jabs:
                jump = op.jabs

            # if we have an op that makes a jump, we need special processing
            if jump is not None:
                # we're going to jump, so remember the index of the next op and current depth
                idx_stack.append((idx + 1, depth, copy.copy(seen)))

                if op_cls in (OP_SETUP_EXCEPT, OP_SETUP_FINALLY):
                    depth += 3
                    if depth > maxdepth:
                        maxdepth = depth
                elif op_cls is OP_FOR_ITER:
                    depth -= 2

                idx = ops.index(jump)

                # if we're jumping completely outside of the current code, the next op is
                # likely dead code, so we don't want our index stack to point back to it
                if op_cls in (OP_JUMP_ABSOLUTE, OP_JUMP_FORWARD):
                    idx_stack.pop()

                continue

            # advance
            idx += 1
            if idx >= ops_len or op_cls == OP_RETURN_VALUE:
                try:
                    idx, depth, seen = idx_stack.pop()
                except IndexError:
                    break

        return maxdepth

    def _calc_flags(self):
        ops_set = {op.__class__ for op in self.ops}

        flags = 0

        if not len(_OPTIMIZE_NEG_SET & ops_set):
            flags |= CO_OPTIMIZED

        if OP_YIELD_VALUE in ops_set:
            flags |= CO_GENERATOR

        if OP_YIELD_FROM is not None:
            if OP_YIELD_FROM in ops_set:
                flags |= CO_GENERATOR

        if not len(opcodes.FREE_OPS & ops_set) and not self.has_class_freevar:
            flags |= CO_NOFREE

        if self.vararg:
            flags |= CO_VARARGS

        if self.varkwarg:
            flags |= CO_VARKEYWORDS

        if self.newlocals:
            flags |= CO_NEWLOCALS

        return flags

    def to_code(self):
        '''Compiles the code object to the standard python's code object.'''

        # Fill 'co_varnames' with function arguments names first.  It will be
        # extended with the names of local variables in later stages.
        # Initialize 'co_argcount', since we know everything we need about
        # arguments at this point.
        #
        co_varnames = OrderedSet()
        if self.args:
            co_varnames.update(self.args)
        co_argcount = len(co_varnames)
        if self.kwonlyargs:
            co_varnames.update(self.kwonlyargs)
        co_kwonlyargcount = len(self.kwonlyargs)
        if self.vararg:
            co_varnames.add(self.vararg)
        if self.varkwarg:
            co_varnames.add(self.varkwarg)

        co_names = OrderedSet()
        co_cellvars = OrderedSet()
        co_freevars = OrderedSet()
        co_consts = OrderedSet([self.docstring])

        if self.has_class_freevar:
            # Should be the last cell always
            #
            co_freevars.add((_MAX_CELL_WEIGHT + 1, '__class__'))

        # Stage 1.
        # Go through all opcodes and fill up 'co_varnames', 'co_names',
        # 'co_freevars', 'co_cellvars' and 'co_consts'
        #
        # Later, in Stage 2 we'll need the exact indexes of values in
        # those lists.
        #
        for op in self.ops:
            if op.has_local:
                co_varnames.add(op.local)
            elif op.has_name:
                co_names.add(op.name)
            elif op.has_free:
                arg = getattr(op, 'arg', _MAX_CELL_WEIGHT)
                try:
                    cell = op.cell
                except AttributeError:
                    co_freevars.add((arg, op.free))
                else:
                    co_cellvars.add((arg, cell))
            elif op.has_const:
                co_consts.add(op.const)

        # Now we have the following lists at their final state.
        #
        co_varnames = tuple(co_varnames)
        co_names = tuple(co_names)
        co_cellvars = tuple((cell for arg, cell in sorted(co_cellvars, key=lambda el: el[0])))
        co_freevars = tuple((free for arg, free in sorted(co_freevars, key=lambda el: el[0])))
        co_consts = tuple(co_consts)

        # Stage 2.
        # Now we start to write opcodes to the 'code' bytearray.
        # Since we don't know the final addresses of all commands, we can't yet
        # resolve jumps, so we memorize positions where to insert jump addresses
        # in 'jumps' list, write 0s for now, and in Stage 3 we will write the
        # already known addresses in place of those 0s.
        #
        # At this stage we also calculate 'co_lnotab'.
        #
        code = bytearray()

        # Addresses of all opcodes to resolve jumps later
        addrs = {}
        # Where to write jump addresses later
        jumps = []

        # A marker.
        no_arg = object()

        len_co_cellvars = len(co_cellvars)
        lnotab = bytearray()
        lastlineno = self.firstlineno
        lastlinepos = 0

        for op in self.ops:
            addr = len(code)
            addrs[op] = addr

            # Update 'co_lnotab' if we have a new line
            try:
                line = op.lineno
            except AttributeError:
                pass
            else:
                if line is not None:
                    inc_line = line - lastlineno
                    inc_pos = addr - lastlinepos
                    lastlineno = line
                    lastlinepos = addr

                    if inc_line == inc_pos == 0:
                        lnotab.extend((0, 0))
                    else:
                        # See 'Objects/lnotab_notes.txt' (in python source code)
                        # for details
                        #
                        while inc_pos > 255:
                            lnotab.extend((255, 0))
                            inc_pos -= 255
                        while inc_line > 255:
                            lnotab.extend((inc_pos, 255))
                            inc_pos = 0
                            inc_line -= 255
                        if inc_pos != 0  or inc_line != 0:
                            lnotab.extend((inc_pos, inc_line))

            arg = no_arg

            if op.has_local:
                arg = co_varnames.index(op.local)
            elif op.has_name:
                arg = co_names.index(op.name)
            elif op.has_free:
                try:
                    cell = op.cell
                except AttributeError:
                    # We adjust position with 'len_co_cellvars', as the same opcode
                    # can sometimes have its 'free' argument be set to either a 'cell'
                    # or 'free' variable
                    #
                    arg = co_freevars.index(op.free) + len_co_cellvars
                else:
                    arg = co_cellvars.index(cell)
            elif op.has_const:
                arg = co_consts.index(op.const)
            elif op.has_jrel:
                # Don't know the address yet. Will resolve that in Stage 3.
                # For now just write 0s.
                #
                jumps.append(('rel', addr, op.jrel))
                arg = 0
            elif op.has_jabs:
                # Don't know the address yet. Will resolve that in Stage 3.
                # For now just write 0s.
                #
                jumps.append(('abs', addr, op.jabs))
                arg = 0
            elif op.has_arg:
                arg = op.arg

            if arg is not no_arg:
                if arg > 0xFFFF:
                    code.append(OP_EXTENDED_ARG.code)
                    code.append((arg >> 16) & 0xFF)
                    code.append((arg >> 24) & 0xFF)
                code.append(op.code)
                code.append(arg & 0xFF)
                code.append((arg >> 8) & 0xFF)
            else:
                code.append(op.code)

        # 'co_lnotab' is ready to go.
        #
        co_lnotab = bytes(lnotab)

        # Stage 3.
        # Resolve jump addresses.
        #
        for jump in jumps:
            to = addrs[jump[2]]
            if jump[0] == 'rel':
                to -= (jump[1] + 3)

            if to > 0xFFFF:
                raise OverflowError('extended jumps are not currently supported')

            code[jump[1] + 1] = to & 0xFF
            code[jump[1] + 2] = (to >> 8) & 0xFF

        # Stage 4.
        # Assemble the new code object.

        co_code = bytes(code)

        return types.CodeType(co_argcount,
                              co_kwonlyargcount,
                              len(co_varnames),
                              self._calc_stack_size(),
                              self._calc_flags(),
                              co_code,
                              co_consts,
                              co_names,
                              co_varnames,
                              self.filename,
                              self.name,
                              self.firstlineno,
                              co_lnotab,
                              co_freevars,
                              co_cellvars)

    @classmethod
    def from_code(cls, pycode):
        '''Creates ``Code`` object instance from python code object'''

        assert isinstance(pycode, types.CodeType)

        co_code = pycode.co_code
        cell_len = len(pycode.co_cellvars)
        n = len(co_code)
        i = 0

        table = {} # We use this to resolve jumps to real opcode objects in Stage 2
        ops = []

        OPMAP = opcodes.OPMAP

        lines = dict(dis.findlinestarts(pycode))

        extended_arg = 0

        # Stage 1.
        # Transform serialized binary list of python opcodes to
        # a high level representation - list of opcode objects
        # defined in the 'opcodes' module
        #
        while i < n:
            op_cls = OPMAP[co_code[i]]
            op = op_cls()

            try:
                line = lines[i]
            except KeyError:
                pass
            else:
                op.lineno = line

            table[i] = op

            i += 1

            if op.has_arg:
                arg = op.arg = co_code[i] + co_code[i + 1] * 256 + extended_arg
                extended_arg = 0
                i += 2

                if op_cls is OP_EXTENDED_ARG:
                    extended_arg = arg << 16
                    # Don't need this opcode in our code object.  We'll write it back
                    # later in 'to_code' automatically if needed.
                    #
                    continue
                elif op_cls.has_jrel:
                    op.jrel = i + arg
                elif op_cls.has_jabs:
                    op.jabs = arg
                elif op_cls.has_name:
                    op.name = pycode.co_names[arg]
                elif op_cls.has_local:
                    op.local = pycode.co_varnames[arg]
                elif op_cls.has_const:
                    op.const = pycode.co_consts[arg]
                elif op_cls.has_free:
                    try:
                        op.cell = pycode.co_cellvars[arg]
                    except IndexError:
                        op.free = pycode.co_freevars[arg - cell_len]

            ops.append(op)

        # Stage 2.
        # Resolve jump addresses to opcode objects.
        #
        for op in ops:
            if op.has_jrel:
                op.jrel = table[op.jrel]
            elif op.has_jabs:
                op.jabs = table[op.jabs]

        # Stage 3.
        # Unwind python's arguments mess.  All arguments names are stored in the
        # 'co_varnames' array, with the names of local variables too.

        varnames = pycode.co_varnames
        argcount = pycode.co_argcount

        argstop = argcount
        args = OrderedSet(varnames[:argstop])

        kwonlyargs = OrderedSet(varnames[argstop:argstop + pycode.co_kwonlyargcount])
        argstop += pycode.co_kwonlyargcount

        # Handle '*args' type of argument
        vararg = None
        if pycode.co_flags & CO_VARARGS:
            vararg = varnames[argstop]
            argstop += 1

        # Handle '**kwargs' type of argument
        varkwarg = None
        if pycode.co_flags & CO_VARKEYWORDS:
            varkwarg = varnames[argstop]

        # Docstring is just a first string element in the 'co_consts' tuple
        #
        docstring = None
        co_consts = pycode.co_consts
        if len(co_consts) and isinstance(co_consts[0], str):
            docstring = co_consts[0]

        obj = cls(ops,
                  vararg=vararg,
                  varkwarg=varkwarg,
                  newlocals=bool(pycode.co_flags & CO_NEWLOCALS),
                  filename=pycode.co_filename,
                  firstlineno=pycode.co_firstlineno,
                  name=pycode.co_name,
                  args=args,
                  kwonlyargs=kwonlyargs,
                  docstring=docstring,
                  has_class_freevar='__class__' in pycode.co_freevars)

        return obj
