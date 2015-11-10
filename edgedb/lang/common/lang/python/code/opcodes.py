##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Contains definitions and specifications of CPython VM opcodes.
Used by :py:class:`metamagic.utils.lang.python.code.Code` to enable
python code object augmentation."""


import sys
import opcode as _opcode

from metamagic.utils import slots
from metamagic.utils.datastructures import Void as _no_arg


OPMAP       = {}

OPS         = set()
FREE_OPS    = set()
ARG_OPS     = set()
LOCAL_OPS   = set()
CONST_OPS   = set()
NAME_OPS    = set()
JREL_OPS    = set()
JABS_OPS    = set()


class OpCodeMeta(slots.SlotsMeta):
    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        try:
            code = dct['code']
        except KeyError:
            raise TypeError('opcode {} defined without code value'.format(name))
        else:
            if code != -1:
                assert code not in OPMAP, 'unrecognized opcode: {}'.format(code)
                opname = _opcode.opname[code]
                assert opname == name, 'opcode name does not match: {!r}, expected: {!r}'\
                                                .format(opname, name)
                OPMAP[code] = cls

                cls.opname = name

                OPS.add(cls)

                if cls.has_arg:
                    ARG_OPS.add(cls)

                if cls.has_local:
                    LOCAL_OPS.add(cls)

                if cls.has_const:
                    CONST_OPS.add(cls)

                if cls.has_name:
                    NAME_OPS.add(cls)

                if cls.has_free:
                    FREE_OPS.add(cls)

                if cls.has_jrel:
                    JREL_OPS.add(cls)

                if cls.has_jabs:
                    JABS_OPS.add(cls)

        return cls


class OpCode(metaclass=OpCodeMeta):
    __slots__       = ('lineno',)

    def __init__(self, lineno=_no_arg):
        if lineno is not _no_arg:
            self.lineno = lineno

    code            = -1 # base class

    has_arg         = False
    has_local       = False
    has_const       = False
    has_name        = False
    has_compare     = False
    has_free        = False
    has_jrel        = False
    has_jabs        = False

    def __repr__(self):
        return self.opname

    @property
    def stack_effect(self):
        raise NotImplementedError('undefined stack effect')


class ArgOpCode(OpCode):
    __slots__       = ('arg',)

    def __init__(self, arg=_no_arg, **kw):
        if arg is not _no_arg:
            self.arg = arg
        OpCode.__init__(self, **kw)

    code            = -1 # base class

    has_arg         = True
    arg_doc         = None

    def __repr__(self):
        r = self.opname.ljust(15)

        prop = ''
        try:
            try:
                line = self.lineno
            except AttributeError:
                prop += ' ' * 7
            else:
                prop += '({})'.format(line).ljust(7)

            if self.has_name:
                prop += 'name : {}'.format(self.name)
            elif self.has_local:
                prop += 'local: {}'.format(self.local)
            elif self.has_const:
                prop += 'const: {}'.format(self.const)
            elif self.has_free:
                try:
                    prop += 'free : {}'.format(self.free)
                except AttributeError:
                    prop += 'cell : {}'.format(self.cell)
            elif self.has_arg:
                prop += 'arg  : {}'.format(self.arg)
        except AttributeError:
            pass

        if prop is not None:
            r += prop

        return r

    def _nargs(self, arg):
        return (arg & 0xFF) + 2 * ((arg >> 8) & 0xFF)


class NameOpCode(ArgOpCode):
    __slots__       = ('name',)
    code            = -1 # base class
    has_name        = True

    def __init__(self, name=_no_arg, **kw):
        if name is not _no_arg:
            self.name = name
        ArgOpCode.__init__(self, **kw)


class FreeNameOpCode(ArgOpCode):
    __slots__       = ('free', 'cell')
    code            = -1 # base class
    has_free        = True

    def __init__(self, free=_no_arg, cell=_no_arg, **kw):
        if free is not _no_arg:
            self.free = free
        if cell is not _no_arg:
            self.cell = cell
        ArgOpCode.__init__(self, **kw)

class LocalNameOpCode(ArgOpCode):
    __slots__       = ('local',)
    code            = -1 # base class
    has_local       = True

    def __init__(self, local=_no_arg, **kw):
        if local is not _no_arg:
            self.local = local
        ArgOpCode.__init__(self, **kw)


class ConstNameOpCode(ArgOpCode):
    __slots__       = ('const',)
    code            = -1 # base class
    has_const       = True

    def __init__(self, const=_no_arg, **kw):
        if const is not _no_arg:
            self.const = const
        ArgOpCode.__init__(self, **kw)


class JRelOpCode(ArgOpCode):
    __slots__       = ('jrel',)
    code            = -1 # base class
    has_jrel        = True

    def __init__(self, jrel=_no_arg, **kw):
        if jrel is not _no_arg:
            self.jrel = jrel
        ArgOpCode.__init__(self, **kw)


class JAbsOpCode(ArgOpCode):
    __slots__       = ('jabs',)
    code            = -1 # base class
    has_jabs        = True

    def __init__(self, jabs=_no_arg, **kw):
        if jabs is not _no_arg:
            self.jabs = jabs
        ArgOpCode.__init__(self, **kw)


# `stack_effect` values and algorithms to calculate it are taken from
# 'Python/compile.c' opcode_stack_effect() function.

if sys.version_info[:2] < (3, 3):
    # STOP_CODE has been removed in 3.3
    class STOP_CODE(OpCode):
        __slots__       = ()
        code            = 0
        stack_effect    = 0


class POP_TOP(OpCode):
    __slots__       = ()
    code            = 1
    stack_effect    = -1


class ROT_TWO(OpCode):
    __slots__       = ()
    code            = 2
    stack_effect    = 0


class ROT_THREE(OpCode):
    __slots__       = ()
    code            = 3
    stack_effect    = 0


class DUP_TOP(OpCode):
    __slots__       = ()
    code            = 4
    stack_effect    = 1


class DUP_TOP_TWO(OpCode):
    __slots__       = ()
    code            = 5
    stack_effect    = 2


class NOP(OpCode):
    __slots__       = ()
    code            = 9
    stack_effect    = 0


class UNARY_POSITIVE(OpCode):
    __slots__       = ()
    code            = 10
    stack_effect    = 0


class UNARY_NEGATIVE(OpCode):
    __slots__       = ()
    code            = 11
    stack_effect    = 0


class UNARY_NOT(OpCode):
    __slots__       = ()
    code            = 12
    stack_effect    = 0


class UNARY_INVERT(OpCode):
    __slots__       = ()
    code            = 15
    stack_effect    = 0


class BINARY_POWER(OpCode):
    __slots__       = ()
    code            = 19
    stack_effect    = -1


class BINARY_MULTIPLY(OpCode):
    __slots__       = ()
    code            = 20
    stack_effect    = -1


class BINARY_MODULO(OpCode):
    __slots__       = ()
    code            = 22
    stack_effect    = -1


class BINARY_ADD(OpCode):
    __slots__       = ()
    code            = 23
    stack_effect    = -1


class BINARY_SUBTRACT(OpCode):
    __slots__       = ()
    code            = 24
    stack_effect    = -1


class BINARY_SUBSCR(OpCode):
    __slots__       = ()
    code            = 25
    stack_effect    = -1


class BINARY_FLOOR_DIVIDE(OpCode):
    __slots__       = ()
    code            = 26
    stack_effect    = -1


class BINARY_TRUE_DIVIDE(OpCode):
    __slots__       = ()
    code            = 27
    stack_effect    = -1


class INPLACE_FLOOR_DIVIDE(OpCode):
    __slots__       = ()
    code            = 28
    stack_effect    = -1


class INPLACE_TRUE_DIVIDE(OpCode):
    __slots__       = ()
    code            = 29
    stack_effect    = -1


if sys.version_info[:2] < (3, 5):
    class STORE_MAP(OpCode):
        __slots__       = ()
        code            = 54
        stack_effect    = -2


class INPLACE_ADD(OpCode):
    __slots__       = ()
    code            = 55
    stack_effect    = -1


class INPLACE_SUBTRACT(OpCode):
    __slots__       = ()
    code            = 56
    stack_effect    = -1


class INPLACE_MULTIPLY(OpCode):
    __slots__       = ()
    code            = 57
    stack_effect    = -1


class INPLACE_MODULO(OpCode):
    __slots__       = ()
    code            = 59
    stack_effect    = -1


class STORE_SUBSCR(OpCode):
    __slots__       = ()
    code            = 60
    stack_effect    = -3


class DELETE_SUBSCR(OpCode):
    __slots__       = ()
    code            = 61
    stack_effect    = -2


class BINARY_LSHIFT(OpCode):
    __slots__       = ()
    code            = 62
    stack_effect    = -1


class BINARY_RSHIFT(OpCode):
    __slots__       = ()
    code            = 63
    stack_effect    = -1


class BINARY_AND(OpCode):
    __slots__       = ()
    code            = 64
    stack_effect    = -1


class BINARY_XOR(OpCode):
    __slots__       = ()
    code            = 65
    stack_effect    = -1


class BINARY_OR(OpCode):
    __slots__       = ()
    code            = 66
    stack_effect    = -1


class INPLACE_POWER(OpCode):
    __slots__       = ()
    code            = 67
    stack_effect    = -1


class GET_ITER(OpCode):
    __slots__       = ()
    code            = 68
    stack_effect    = 0


if sys.version_info[:2] <= (3, 3):
    class STORE_LOCALS(OpCode):
        __slots__       = ()
        code            = 69
        stack_effect    = -1
elif sys.version_info[:2] >= (3, 5):
    class GET_YIELD_FROM_ITER(OpCode):
        __slots__       = ()
        code            = 69
        stack_effect    = 0


class PRINT_EXPR(OpCode):
    __slots__       = ()
    code            = 70
    stack_effect    = -1


class LOAD_BUILD_CLASS(OpCode):
    __slots__       = ()
    code            = 71
    stack_effect    = 1


if sys.version_info[:2] >= (3, 3):
    class YIELD_FROM(OpCode):
        __slots__       = ()
        code            = 72
        stack_effect    = -1


class INPLACE_LSHIFT(OpCode):
    __slots__       = ()
    code            = 75
    stack_effect    = -1


class INPLACE_RSHIFT(OpCode):
    __slots__       = ()
    code            = 76
    stack_effect    = -1


class INPLACE_AND(OpCode):
    __slots__       = ()
    code            = 77
    stack_effect    = -1


class INPLACE_XOR(OpCode):
    __slots__       = ()
    code            = 78
    stack_effect    = -1


class INPLACE_OR(OpCode):
    __slots__       = ()
    code            = 79
    stack_effect    = -1


class BREAK_LOOP(OpCode):
    __slots__       = ()
    code            = 80
    stack_effect    = 0


if sys.version_info[:2] < (3, 5):
    class WITH_CLEANUP(OpCode):
        __slots__       = ()
        code            = 81
        stack_effect    = -1 # XXX: Sometimes more
else:
    class WITH_CLEANUP_START(OpCode):
        __slots__       = ()
        code            = 81
        stack_effect    = 1

    class WITH_CLEANUP_FINISH(OpCode):
        __slots__       = ()
        code            = 82
        stack_effect    = -1


class RETURN_VALUE(OpCode):
    __slots__       = ()
    code            = 83
    stack_effect    = -1


class IMPORT_STAR(OpCode):
    __slots__       = ()
    code            = 84
    stack_effect    = -1


class YIELD_VALUE(OpCode):
    __slots__       = ()
    code            = 86
    stack_effect    = 0


class POP_BLOCK(OpCode):
    __slots__       = ()
    code            = 87
    stack_effect    = 0


class END_FINALLY(OpCode):
    __slots__       = ()
    code            = 88
    stack_effect    = -1 # or -2 or -3 if exception occurred


class POP_EXCEPT(OpCode):
    __slots__       = ()
    code            = 89
    stack_effect    = 0 # -3 except if bad bytecode


class STORE_NAME(NameOpCode):
    __slots__       = ()
    code            = 90
    stack_effect    = -1
    arg_doc         = 'Index in name list'


class DELETE_NAME(NameOpCode):
    __slots__       = ()
    code            = 91
    stack_effect    = 0


class UNPACK_SEQUENCE(ArgOpCode):
    __slots__       = ()
    code            = 92
    arg_doc         = 'Number of tuple items'

    @property
    def stack_effect(self):
        return self.arg - 1


class FOR_ITER(JRelOpCode):
    __slots__       = ()
    code            = 93
    stack_effect    = 1 # or -1, at end of iterator


class UNPACK_EX(ArgOpCode):
    __slots__       = ()
    code            = 94

    @property
    def stack_effect(self):
        return (self.arg & 0xFF) + (self.arg >> 8)


class STORE_ATTR(NameOpCode):
    __slots__       = ()
    code            = 95
    stack_effect    = -2


class DELETE_ATTR(NameOpCode):
    __slots__       = ()
    code            = 96
    stack_effect    = -1


class STORE_GLOBAL(NameOpCode):
    __slots__       = ()
    code            = 97
    stack_effect    = -1


class DELETE_GLOBAL(NameOpCode):
    __slots__       = ()
    code            = 98
    stack_effect    = 0


class LOAD_CONST(ConstNameOpCode):
    __slots__       = ()
    code            = 100
    stack_effect    = 1


class LOAD_NAME(NameOpCode):
    __slots__       = ()
    code            = 101
    stack_effect    = 1
    arg_doc         = 'Index in name list'


class BUILD_TUPLE(ArgOpCode):
    __slots__       = ()
    code            = 102
    arg_doc         = 'Number of tuple items'

    @property
    def stack_effect(self):
        return 1 - self.arg


class BUILD_LIST(ArgOpCode):
    __slots__       = ()
    code            = 103
    arg_doc         = 'Number of list items'

    @property
    def stack_effect(self):
        return 1 - self.arg


class BUILD_SET(ArgOpCode):
    __slots__       = ()
    code            = 104
    arg_doc         = 'Number of set items'

    @property
    def stack_effect(self):
        return 1 - self.arg


class BUILD_MAP(ArgOpCode):
    __slots__       = ()
    code            = 105
    stack_effect    = 1
    arg_doc         = 'Number of dict entries (upto 255)'


class LOAD_ATTR(NameOpCode):
    __slots__       = ()
    code            = 106
    stack_effect    = 0
    arg_doc         = 'Index in name list'


class COMPARE_OP(ArgOpCode):
    __slots__       = ()
    code            = 107
    stack_effect    = -1
    arg_doc         = 'Comparison operator'
    has_compare     = True


class IMPORT_NAME(NameOpCode):
    __slots__       = ()
    code            = 108
    stack_effect    = -1
    arg_doc         = 'Index in name list'


class IMPORT_FROM(NameOpCode):
    __slots__       = ()
    code            = 109
    stack_effect    = 1
    arg_doc         = 'Index in name list'


class JUMP_FORWARD(JRelOpCode):
    __slots__       = ()
    code            = 110
    stack_effect    = 0
    arg_doc         = 'Number of bytes to skip'


class JUMP_IF_FALSE_OR_POP(JAbsOpCode):
    __slots__       = ()
    code            = 111
    stack_effect    = 0 # -1 if jump not taken
    arg_doc         = 'Target byte offset from beginning of code'


class JUMP_IF_TRUE_OR_POP(JAbsOpCode):
    __slots__       = ()
    code            = 112
    stack_effect    = 0 # -1 if jump not taken
    arg_doc         = 'Target byte offset from beginning of code'


class JUMP_ABSOLUTE(JAbsOpCode):
    __slots__       = ()
    code            = 113
    stack_effect    = 0


class POP_JUMP_IF_FALSE(JAbsOpCode):
    __slots__       = ()
    code            = 114
    stack_effect    = -1


class POP_JUMP_IF_TRUE(JAbsOpCode):
    __slots__       = ()
    code            = 115
    stack_effect    = -1


class LOAD_GLOBAL(NameOpCode):
    __slots__       = ()
    code            = 116
    stack_effect    = 1
    arg_doc         = 'Index in name list'


class CONTINUE_LOOP(JAbsOpCode):
    __slots__       = ()
    code            = 119
    stack_effect    = 0
    arg_doc         = 'Target address'


class SETUP_LOOP(JRelOpCode):
    __slots__       = ()
    code            = 120
    stack_effect    = 0
    arg_doc         = 'Distance to target address'


class SETUP_EXCEPT(JRelOpCode):
    __slots__       = ()
    code            = 121
    stack_effect    = 6 # can push 3 values for the new exception
                        # + 3 others for the previous exception state


class SETUP_FINALLY(JRelOpCode):
    __slots__       = ()
    code            = 122
    stack_effect    = 6 # can push 3 values for the new exception
                        # + 3 others for the previous exception state


class LOAD_FAST(LocalNameOpCode):
    __slots__       = ()
    code            = 124
    stack_effect    = 1
    arg_doc         = 'Local variable number'


class STORE_FAST(LocalNameOpCode):
    __slots__       = ()
    code            = 125
    stack_effect    = -1
    arg_doc         = 'Local variable number'


class DELETE_FAST(LocalNameOpCode):
    __slots__       = ()
    code            = 126
    stack_effect    = 0
    arg_doc         = 'Local variable number'


class RAISE_VARARGS(ArgOpCode):
    __slots__       = ()
    code            = 130
    arg_doc         = 'Number of raise arguments (1, 2, or 3)'

    @property
    def stack_effect(self):
        return -self.arg


class CALL_FUNCTION(ArgOpCode):
    __slots__       = ()
    code            = 131
    arg_doc         = '#args + (#kwargs << 8)'

    @property
    def stack_effect(self):
        return -(self._nargs(self.arg))


class MAKE_FUNCTION(ArgOpCode):
    __slots__       = ()
    code            = 132
    arg_doc         = 'Number of args with default values'

    @property
    def stack_effect(self):
        return -1 - (self._nargs(self.arg)) - ((self.arg >> 16) & 0xFFFF)


class BUILD_SLICE(ArgOpCode):
    __slots__       = ()
    code            = 133
    arg_doc         = 'Number of items'

    @property
    def stack_effect(self):
        if self.arg == 3:
            return -2
        else:
            return -1


class MAKE_CLOSURE(ArgOpCode):
    __slots__       = ()
    code            = 134

    @property
    def stack_effect(self):
        return -2 - self._nargs(self.arg) - ((self.arg >> 16) & 0xFFFF)


class LOAD_CLOSURE(FreeNameOpCode):
    __slots__       = ()
    code            = 135
    stack_effect    = 1


class LOAD_DEREF(FreeNameOpCode):
    __slots__       = ()
    code            = 136
    stack_effect    = 1


class STORE_DEREF(FreeNameOpCode):
    __slots__       = ()
    code            = 137
    stack_effect    = -1


class DELETE_DEREF(FreeNameOpCode):
    __slots__       = ()
    code            = 138
    stack_effect    = 0


class CALL_FUNCTION_VAR(ArgOpCode):
    __slots__       = ()
    code            = 140
    arg_doc         = '#args + (#kwargs << 8)'

    @property
    def stack_effect(self):
        return -(self._nargs(self.arg)) - 1


class CALL_FUNCTION_KW(ArgOpCode):
    __slots__       = ()
    code            = 141
    arg_doc         = '#args + (#kwargs << 8)'

    @property
    def stack_effect(self):
        return -(self._nargs(self.arg)) - 1


class CALL_FUNCTION_VAR_KW(ArgOpCode):
    __slots__       = ()
    code            = 142
    arg_doc         = '#args + (#kwargs << 8)'

    @property
    def stack_effect(self):
        return -(self._nargs(self.arg)) - 2


class SETUP_WITH(JRelOpCode):
    __slots__       = ()
    code            = 143
    stack_effect    = 7


class EXTENDED_ARG(ArgOpCode):
    __slots__        = ()
    code            = 144


class LIST_APPEND(ArgOpCode):
    __slots__       = ()
    code            = 145
    stack_effect    = -1


class SET_ADD(ArgOpCode):
    __slots__       = ()
    code            = 146
    stack_effect    = -1


class MAP_ADD(ArgOpCode):
    __slots__       = ()
    code            = 147
    stack_effect    = -2


if sys.version_info[:2] >= (3, 4):
    class LOAD_CLASSDEREF(FreeNameOpCode):
        __slots__       = ()
        code            = 148
        stack_effect    = 1


OPS         = frozenset(OPS)
FREE_OPS    = frozenset(FREE_OPS)
ARG_OPS     = frozenset(ARG_OPS)
LOCAL_OPS   = frozenset(LOCAL_OPS)
CONST_OPS   = frozenset(CONST_OPS)
NAME_OPS    = frozenset(NAME_OPS)
JREL_OPS    = frozenset(JREL_OPS)
JABS_OPS    = frozenset(JABS_OPS)
