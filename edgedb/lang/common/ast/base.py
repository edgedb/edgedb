# Partial Copyright 2009 Sprymix Inc.
# Partial Copyright 2008 by Armin Ronacher.
# License: Python License

import functools

class ASTError(Exception):
    pass

class MetaAST(type):
    counter = 0

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        MetaAST.counter += 1

        fields = getattr(cls, '_fields', None)
        if fields is None:
            raise ASTError('%s class does not have _fields attribute')

        code = 'def _init_fields(self):\n'
        code += '\tself._id = %d\n' % MetaAST.counter

        _fields = []
        if fields:
            for field in fields:
                if field.startswith('*'):
                    field = field[1:]
                    code += '\tself.%s = []\n' % field
                else:
                    code += '\tself.%s = None\n' % field

                _fields.append(field)

        context = {}
        exec(code, context)
        func = context['_init_fields']

        setattr(cls, '_fields', _fields)
        setattr(cls, '_init_fields', func)


class AST(object, metaclass=MetaAST):
    _fields = []

    def __init__(self, **kwargs):
        self.parent = None
        self._init_fields()

        for arg, value in kwargs.items():
            if hasattr(self, arg):
                if isinstance(value, AST):
                    value.parent = self
                elif isinstance(value, AST):
                    for v in value:
                        if isinstance(v, AST):
                            v.parent = self

                setattr(self, arg, value)
            else:
                raise ASTError('cannot set attribute "%s" in ast class "%s"' %
                               (arg, self.__class__.__name__))

        if 'parent' in kwargs:
            self.parent = kwargs['parent']

    """
    def __eq__(self, node):
        eq = self.__class__ is node.__class__
        if eq:
            for f in self._fields:
                eq = eq and getattr(self, f) == getattr(node, f)
        return eq
    """


class ASTBlockNode(AST):
    def append_node(self, node):
        if isinstance(node, list):
            for n in node:
                self.append_node(n)
        else:
            node.parent = self
            self.body.append(node)


def iter_fields(node):
    """
    Yield a tuple of ``(fieldname, value)`` for each field in ``node._fields``
    that is present on *node*.
    """
    for field in node._fields:
        if hasattr(node, field):
            yield field, getattr(node, field)


def dump(node, annotate_fields=True, include_attributes=False):
    """
    Return a formatted dump of the tree in *node*.  This is mainly useful for
    debugging purposes.  The returned string will show the names and the values
    for fields.  This makes the code impossible to evaluate, so if evaluation is
    wanted *annotate_fields* must be set to False.  Attributes such as line
    numbers and column offsets are not dumped by default.  If this is wanted,
    *include_attributes* can be set to True.
    """
    def _format(node):
        if isinstance(node, AST):
            fields = [(a, _format(b)) for a, b in iter_fields(node)]
            rv = '%s(%s' % (node.__class__.__name__, ', '.join(
                ('%s=%s' % field for field in fields)
                if annotate_fields else
                (b for a, b in fields)
            ))
            if include_attributes and node._attributes:
                rv += fields and ', ' or ' '
                rv += ', '.join('%s=%s' % (a, _format(getattr(node, a)))
                                for a in node._attributes)
            return rv + ')'
        elif isinstance(node, list):
            return '[%s]' % ', '.join(_format(x) for x in node)
        return repr(node)
    if not isinstance(node, AST):
        raise TypeError('expected AST, got %r' % node.__class__.__name__)
    return _format(node)


class NodeVisitor(object):
    """
    A node visitor base class that walks the abstract syntax tree and calls a
    visitor function for every node found.  This function may return a value
    which is forwarded by the `visit` method.

    This class is meant to be subclassed, with the subclass adding visitor
    methods.

    Per default the visitor functions for the nodes are ``'visit_'`` +
    class name of the node.  So a `TryFinally` node visit function would
    be `visit_TryFinally`.  This behavior can be changed by overriding
    the `visit` method.  If no visitor function exists for a node
    (return value `None`) the `generic_visit` visitor is used instead.

    Don't use the `NodeVisitor` if you want to apply changes to nodes during
    traversing.  For this a special visitor exists (`NodeTransformer`) that
    allows modifications.
    """

    def visit(self, node):
        method = 'visit_' + node.__class__.__name__
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        for field, value in iter_fields(node):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, AST):
                        self.visit(item)
            elif isinstance(value, AST):
                self.visit(value)


class NodeTransformer(NodeVisitor):
    """
    A :class:`NodeVisitor` subclass that walks the abstract syntax tree and
    allows modification of nodes.

    The `NodeTransformer` will walk the AST and use the return value of the
    visitor methods to replace or remove the old node.  If the return value of
    the visitor method is ``None``, the node will be removed from its location,
    otherwise it is replaced with the return value.  The return value may be the
    original node in which case no replacement takes place.

    Here is an example transformer that rewrites all occurrences of name lookups
    (``foo``) to ``data['foo']``::

       class RewriteName(NodeTransformer):

           def visit_Name(self, node):
               return copy_location(Subscript(
                   value=Name(id='data', ctx=Load()),
                   slice=Index(value=Str(s=node.id)),
                   ctx=node.ctx
               ), node)

    Keep in mind that if the node you're operating on has child nodes you must
    either transform the child nodes yourself or call the :meth:`generic_visit`
    method for the node first.

    For nodes that were part of a collection of statements (that applies to all
    statement nodes), the visitor may also return a list of nodes rather than
    just a single node.

    Usually you use the transformer like this::

       node = YourTransformer().visit(node)
    """

    def generic_visit(self, node):
        for field, old_value in iter_fields(node):
            old_value = getattr(node, field, None)
            if isinstance(old_value, list):
                new_values = []
                for value in old_value:
                    if isinstance(value, AST):
                        value = self.visit(value)
                        if value is None:
                            continue
                        elif not isinstance(value, AST):
                            new_values.extend(value)
                            continue
                    new_values.append(value)

                for value in new_values:
                    value.parent = node

                old_value[:] = new_values

            elif isinstance(old_value, AST):
                new_node = self.visit(old_value)
                if new_node is None:
                    delattr(node, field)
                else:
                    new_node.parent = node
                    setattr(node, field, new_node)

        return node


    def find_child(self, node, test_func):
        for field, value in iter_fields(node):
            if isinstance(value, list):
                for n in value:
                    if test_func(n):
                        return n

                    _n = self.find_child(n, test_func)
                    if _n is not None:
                        return _n

            elif isinstance(value, AST):
                if test_func(value):
                    return value
                else:
                    _n = self.find_child(value, test_func)
                    if _n is not None:
                        return _n


    def replace_child(self, child, new_child):
        if child.parent is None:
            raise ASTError('ast node does not have parent')

        node = child.parent

        for field, value in iter_fields(node):
            if isinstance(value, list):
                for i in range(0, len(value)):
                    if value[i] == child:
                        value[i] = new_child
                        return True

            elif isinstance(value, AST):
                if value == child:
                    setattr(node, field, new_child)
                    return True
