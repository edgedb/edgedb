##
# Portions Copyright (c) 2008-2010 Sprymix Inc.
# Portions Copyright (c) 2008 Armin Ronacher.
# All rights reserved.
#
# This code is licensed under the PSFL license.
##


import re as regex

import semantix.utils.ast.base
from semantix.utils.io import terminal

def dump(node, annotate_fields=True, include_attributes=False):
    visited = {}
    """
    Return a formatted dump of the tree in *node*.  This is mainly useful for
    debugging purposes.  The returned string will show the names and the values
    for fields.  This makes the code impossible to evaluate, so if evaluation is
    wanted *annotate_fields* must be set to False.  Attributes such as line
    numbers and column offsets are not dumped by default.  If this is wanted,
    *include_attributes* can be set to True.
    """
    def _format(node):
        if isinstance(node, semantix.utils.ast.base.AST) and id(node) in visited:
            return _node_recursion_rep(node)
        visited[id(node)] = True

        if isinstance(node, semantix.utils.ast.base.AST):
            fields = [(a, _format(b)) for a, b in semantix.utils.ast.base.iter_fields(node)]
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
    if not isinstance(node, semantix.utils.ast.base.AST):
        return str(node)
    else:
        return _format(node)


def pretty_dump(node, identation_size=4, width=80, colorize=False, field_mask=None):
    visited = {}

    if field_mask is not None:
        mask_re = regex.compile(field_mask)
    else:
        mask_re = None

    def highlight(type, str):
        if colorize:
            if type == 'NODE_NAME':
                return terminal.colorize(str, fg='red', opts=('bold',))
            elif type == 'NODE_FIELD':
                return terminal.colorize(str, fg='black', opts=('bold',))
            elif type == 'LITERAL':
                return terminal.colorize(str, fg='cyan', opts=('bold',))
            elif type == 'RECURSION':
                return terminal.colorize(str, fg='white', bg='red')

        else:
            return str

    def _dump(node):
        visited = {}

        def _format(node):
            if isinstance(node, semantix.utils.ast.base.AST) and id(node) in visited:
                return highlight('RECURSION', _node_recursion_rep(node))
            visited[id(node)] = True

            if isinstance(node, semantix.utils.ast.base.AST):
                fields = [(a, _format(b)) for a, b in semantix.utils.ast.base.iter_fields(node) \
                          if mask_re is None or not mask_re.match(a)]

                rv = '%s(%s' % (highlight('NODE_NAME', node.__class__.__name__), ', '.join(
                    ('%s=%s' % (highlight('NODE_FIELD', field[0]), field[1]) for field in fields)
                ))
                return rv + ')'
            elif isinstance(node, list):
                return '[%s]' % ', '.join(_format(x) for x in node)
            elif isinstance(node, (set, frozenset)):
                return '{%s}' % ', '.join(_format(x) for x in node)
            return highlight('LITERAL', repr(node))
        if not isinstance(node, semantix.utils.ast.base.AST):
            return str(node)
        else:
            return _format(node)

    """
    returns: one line = True, multiline = False
    """
    def _format(node, identation=0, force_multiline=False):
        if isinstance(node, semantix.utils.ast.base.AST) and id(node) in visited:
            if colorize:
                return (1, highlight('RECURSION', _node_recursion_rep(node)))
            else:
                return (1, _node_recursion_rep(node))
        visited[id(node)] = True

        tab = ' ' * identation_size

        if not force_multiline:
            result = dump(node)
            if len(result) + len(tab) < width:
                return (True, _dump(node) if colorize else result)

        pad = tab * identation
        pad_tab = pad + tab

        result = '%s%s(%x)(\n' % (pad, highlight('NODE_NAME', node.__class__.__name__), id(node))

        for field, value in semantix.utils.ast.base.iter_fields(node):
            if mask_re is not None and mask_re.match(field):
                continue

            if isinstance(value, semantix.utils.ast.base.AST):
                _f = _format(value, identation + 1)

                result += '%s%s = ' % (pad_tab, highlight('NODE_FIELD', field))
                result +=  _f[1].lstrip()

                if not result.endswith('\n'):
                    result += '\n'

            elif isinstance(value, (list, set, frozenset)):
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

        result += '%s)\n' % pad
        return (False, result)

    return _format(node)[1]


def _node_recursion_rep(node):
    name = '%s(%x)' % (node.__class__.__name__, id(node))
    for attr_name in ('name', 'id'):
        attr = getattr(node, attr_name, None)
        if attr is not None:
            name += '(%s=%r)' % (attr_name, attr)
            break
    else:
        name += '()'
    return '<recursion: %s>' % name
