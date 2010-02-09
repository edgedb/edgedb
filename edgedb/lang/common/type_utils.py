##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


__all__ = ['check']

def check(variable, type):
    if not isinstance(type, str):
        raise Exception('check_type: type parameter must be string')

    if variable is None:
        return True

    if type == 'str':
        return isinstance(variable, str)

    if type == 'int':
        return isinstance(variable, int)

    if type == 'float':
        return isinstance(variable, float)

    if type == 'bool':
        return isinstance(variable, bool)

    if type == 'list':
        return isinstance(variable, list)

    if type == 'none':
        return variable is None

    raise Exception('check_type: checking on unknown type: %s' % type)


class ClassFactory(type):
    def __new__(cls, name, bases, dct):
        result = super(ClassFactory, cls).__new__(cls, str(name), bases, dct)
        return result

    def __init__(cls, name, bases, dct):
        super(ClassFactory, cls).__init__(name, bases, dct)
