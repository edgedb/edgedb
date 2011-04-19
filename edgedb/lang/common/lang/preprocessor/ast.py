##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import ast


class PPBase(ast.AST): pass


class PP_Error(PPBase):
    __fields = ['message']

class PP_Warning(PPBase):
    __fields = ['message']

class PP_Include(PPBase):
    __fields = ['package']

class PP_DefineName(PPBase):
    __fields = ['name', ('chunks', list)]

class PP_DefineCallable(PPBase):
    __fields = ['name', ('param', list), ('chunks', list)]

class PP_If(PPBase):
    __fields = ['condition', 'firstblock', ('elifblocks', list), 'elseblock']

class PP_Elif(PPBase):
    __fields = ['condition', 'block']

class PP_Else(PPBase):
    __fields = ['block']

class PP_Ifdef(PPBase):
    __fields = ['name', 'firstblock', ('elifblocks', list), 'elseblock']

class PP_Ifndef(PPBase):
    __fields = ['name', 'firstblock', ('elifblocks', list), 'elseblock']

class PP_Call(PPBase):
    __fields = [('arguments', list)]

class PP_CodeChunk(PPBase):
    __fields = ['string', 'token']

class PP_Quote(PPBase): pass

class PP_Concat(PPBase): pass

class PP_Param(PPBase):
    __fields = ['name']

