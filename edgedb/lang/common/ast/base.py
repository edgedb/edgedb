# Portions Copyright 2009 Sprymix Inc.
# Portions Copyright 2008 by Armin Ronacher.
# License: PSFL

from semantix.ast import dump
import copy

class ASTError(Exception):
    pass

class MetaAST(type):
    counter = 0

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        MetaAST.counter += 1

        fields = list()
        for i in range(0, len(cls.__mro__) - 1):
            lst = getattr(cls, '_' + cls.__mro__[i].__name__ + '__fields', [])
            for item in lst:
                if item not in fields:
                    fields.append(item)

        code = 'def _init_fields(self):\n'
        code += '\tself._id = %d\n' % MetaAST.counter

        _fields = []
        if fields:
            for field in fields:
                if field.startswith('*'):
                    field = field[1:]
                    code += '\tself.%s = list()\n' % field

                elif field.startswith('#'):
                    field = field[1:]
                    code += '\tself.%s = dict()\n' % field

                elif field.startswith('!'):
                    field = field[1:]
                    code += '\tself.%s = set()\n' % field

                else:
                    code += '\tself.%s = None\n' % field

                _fields.append(field)

        context = {}
        exec(code, context)
        func = context['_init_fields']

        setattr(cls, '_fields', _fields)
        setattr(cls, '_init_fields', func)


class AST(object, metaclass=MetaAST):
    __fields = []

    def __init__(self, **kwargs):
        self.parent = None
        self._init_fields()

        for arg, value in kwargs.items():
            if hasattr(self, arg):
                if isinstance(value, AST):
                    value.parent = self
                elif isinstance(value, list):
                    for v in value:
                        if isinstance(v, AST):
                            v.parent = self

                setattr(self, arg, value)
            else:
                raise ASTError('cannot set attribute "%s" in ast class "%s"' %
                               (arg, self.__class__.__name__))

        if 'parent' in kwargs:
            self.parent = kwargs['parent']

    def dump(self, *args, **kwargs):
        if 'pretty' in kwargs and kwargs['pretty']:
            del kwargs['pretty']
            return dump.pretty_dump(self, *args, **kwargs)
        else:
            return dump.dump(self, *args, **kwargs)


class ASTBlockNode(AST):
    __fields = ['*body']

    def append_node(self, node):
        if isinstance(node, list):
            for n in node:
                self.append_node(n)
        else:
            node.parent = self
            self.body.append(node)


def fix_parent_links(node):
    for field, value in iter_fields(node):
        if isinstance(value, list):
            for n in value:
                if isinstance(n, AST):
                    n.parent = node
                    fix_parent_links(n)

        elif isinstance(value, AST):
            value.parent = node
            fix_parent_links(value)

    return node


def iter_fields(node):
    """
    Yield a tuple of ``(fieldname, value)`` for each field in ``node._fields``
    that is present on *node*.
    """
    for field in node._fields:
        if hasattr(node, field):
            yield field, getattr(node, field)
