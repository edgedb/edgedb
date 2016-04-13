##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy
import collections


from metamagic.utils import markup


class ASTError(Exception):
    pass


class Field:
    def __init__(self, name, type_, default, traverse, child_traverse=None, field_hidden=False):
        self.name = name
        self.type = type_
        self.default = default
        self.traverse = traverse
        self.child_traverse = child_traverse if child_traverse is not None else traverse
        self.hidden = field_hidden

class MetaAST(type):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        fields = {}
        if __debug__:
            fields = collections.OrderedDict()

        for parent in cls.__mro__:
            lst = getattr(cls, '_' + parent.__name__ + '__fields', [])
            for field in lst:
                field_name = field
                field_type = None
                field_default = None
                field_traverse = True
                field_child_traverse = None
                field_hidden = False

                if isinstance(field, tuple):
                    field_name = field[0]

                    if len(field) > 1:
                        field_type = field[1]
                    if len(field) > 2:
                        field_default = field[2]
                    else:
                        field_default = field_type

                    if len(field) > 3:
                        field_traverse = field[3]

                    if len(field) > 4:
                        field_child_traverse = field[4]

                    if len(field) > 5:
                        field_hidden = field[5]

                if field_name not in fields:
                    fields[field_name] = Field(field_name, field_type, field_default,
                                               field_traverse, field_child_traverse, field_hidden)

        cls._fields = fields

    def get_field(cls, name):
        return cls._fields.get(name)


class AST(object, metaclass=MetaAST):
    __fields = []

    def __init__(self, **kwargs):
        self.parent = None
        self._init_fields(kwargs)

        # XXX: use weakref here
        for arg, value in kwargs.items():
            if hasattr(self, arg):
                if isinstance(value, AST):
                    value.parent = self
                elif isinstance(value, list):
                    for v in value:
                        if isinstance(v, AST):
                            v.parent = self
                elif isinstance(value, dict):
                    for v in value.values():
                        if isinstance(v, AST):
                            v.parent = self
            else:
                raise ASTError('cannot set attribute "%s" in ast class "%s"' %
                               (arg, self.__class__.__name__))

        if 'parent' in kwargs:
            self.parent = kwargs['parent']

    def _init_fields(self, values):
        for field_name, field  in self.__class__._fields.items():
            if field_name in values:
                value = values[field_name]
            elif field.default is not None:
                if field.default is field.type:
                    value = field.default()
                else:
                    value = field.default
            else:
                value = None

            if __debug__:
                self.check_field_type(field, value)

            # Bypass overloaded setattr
            object.__setattr__(self, field_name, value)

    def __copy__(self):
        copied = self.__class__()
        for field, value in iter_fields(self):
            setattr(copied, field, value)
        return copied

    def __deepcopy__(self, memo):
        copied = self.__class__()
        for field, value in iter_fields(self):
            setattr(copied, field, copy.deepcopy(value, memo))
        return copied

    if __debug__:
        def __setattr__(self, name, value):
            super().__setattr__(name, value)
            field = self._fields.get(name)
            if field:
                self.check_field_type(field, value)

    def check_field_type(self, field, value):
        if field.type and value is not None and not isinstance(value, field.type):
            raise TypeError('%s.%s.%s: expected %s but got %s'
                            % (self.__class__.__module__,
                               self.__class__.__name__,
                               field.name, field.type.__name__, value.__class__.__name__))

    def dump(self):
        markup.dump(self)

    def __copy__(self):
        copied = self.__class__()
        for field, value in iter_fields(self):
            setattr(copied, field, value)
        return copied


@markup.serializer.serializer(handles=AST)
def _serialize_to_markup(ast, *, ctx):
    node = markup.elements.lang.TreeNode(id=id(ast), name=type(ast).__name__)

    for fieldname, field in iter_fields(ast):
        node.add_child(label=fieldname, node=markup.serialize(field, ctx=ctx))

    return node


class LanguageAST(AST):
    def __init__(self, **kwargs):
        self.source_position = (None, -1, -1)
        if 'source_position' in kwargs:
            self.source_position = kwargs['source_position']
            del kwargs['source_position']

        super().__init__(**kwargs)

    def __deepcopy__(self, memo):
        copied = super().__deepcopy__(memo)
        copied.source_position = self.source_position
        return copied


class ASTBlockNode(AST):
    __fields = [('body', list)]

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

        elif isinstance(value, dict):
            for n in value.values():
                if isinstance(n, AST):
                    n.parent = node
                    fix_parent_links(n)

        elif isinstance(value, AST):
            value.parent = node
            fix_parent_links(value)

    return node


def iter_fields(node):
    for f in node._fields:
        try:
            yield f, getattr(node, f)
        except AttributeError:
            pass
