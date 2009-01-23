# Partial Copyright 2009 Sprymix Inc.
# Partial Copyright 2008 by Armin Ronacher.
# License: Python License

import functools
from semantix.utils.io import terminal

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

    def dump(self, *args, **kwargs):
        if 'pretty' in kwargs and kwargs['pretty']:
            del kwargs['pretty']
            return pretty_dump(self, *args, **kwargs)
        else:
            return dump(self, *args, **kwargs)

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


def pretty_dump(node, identation_size=4, width=80, field_lable_width=10, colorize=False):
    def highlight(type, str):
        if colorize:
            if type == 'NODE_NAME':
                return terminal.colorize(str, fg='red', opts=('bold',))
            elif type == 'NODE_FIELD':
                return terminal.colorize(str, fg='black', opts=('bold',))
            elif type == 'LITERAL':
                return terminal.colorize(str, fg='cyan', opts=('bold',))

        else:
            return str

    def _dump(node):
        def _format(node):
            if isinstance(node, AST):
                fields = [(a, _format(b)) for a, b in iter_fields(node)]
                rv = '%s(%s' % (highlight('NODE_NAME', node.__class__.__name__), ', '.join(
                    ('%s=%s' % (highlight('NODE_FIELD', field[0]), field[1]) for field in fields)
                ))
                return rv + ')'
            elif isinstance(node, list):
                return '[%s]' % ', '.join(_format(x) for x in node)
            return highlight('LITERAL', repr(node))
        if not isinstance(node, AST):
            raise TypeError('expected AST, got %r' % node.__class__.__name__)
        return _format(node)

    """
    returns: one line = True, multiline = False
    """
    def _format(node, identation=0, force_multiline=False):
        tab = ' ' * identation_size

        if not force_multiline:
            result = dump(node)
            if len(result) + len(tab) < width:
                return (True, _dump(node) if colorize else result)

        pad = tab * identation
        pad_tab = pad + tab

        result = '%s%s {\n' % (pad, highlight('NODE_NAME', node.__class__.__name__))

        for field, value in iter_fields(node):
            if isinstance(value, AST):
                _f = _format(value, identation + 1)

                result += '%s%s = ' % (pad_tab, highlight('NODE_FIELD', field))
                result +=  _f[1].lstrip()

                if not result.endswith('\n'):
                    result += '\n'

            elif isinstance(value, list):
                cache = {}
                multiline = False
                tmp = ''
                for n in value:
                    _f = _format(n, identation + 2)
                    cache[n] = _f
                    if not _f[0]:
                        multiline = True
                        break
                    tmp += _f[1]
                else:
                    if len(tmp) + len(tab) > width:
                        multiline = True

                if not multiline:
                    result += '%s%s = [%s]\n' % (pad_tab, highlight('NODE_FIELD', field), tmp)
                else:
                    result += '%s%s = [\n' % (pad_tab, highlight('NODE_FIELD', field))
                    for n in value:
                        if n in cache:
                            _f = cache[n]
                        else:
                            _f = _format(n, identation + 2)
                        one_line_result, tmp = _f
                        if one_line_result:
                            tmp = pad_tab + tab + tmp + '\n'
                        result += tmp

                    result += '%s]\n' % pad_tab

            else:
                result += '%s%s = %s\n' % (pad_tab,
                                           highlight('NODE_FIELD', field),
                                           highlight('LITERAL', repr(value)))

        result += '%s}\n' % pad
        return (False, result)

    return _format(node)[1]



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
