# Portions Copyright 2009 Sprymix Inc.
# Portions Copyright 2008 by Armin Ronacher.
# License: Python License

import semantix.ast.base
from semantix.utils.io import terminal

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
        if isinstance(node, semantix.ast.base.AST):
            fields = [(a, _format(b)) for a, b in semantix.ast.base.iter_fields(node)]
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
    if not isinstance(node, semantix.ast.base.AST):
        raise TypeError('expected semantix.ast.base.AST, got %r' % node.__class__.__name__)
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
            if isinstance(node, semantix.ast.base.AST):
                fields = [(a, _format(b)) for a, b in semantix.ast.base.iter_fields(node)]
                rv = '%s(%s' % (highlight('NODE_NAME', node.__class__.__name__), ', '.join(
                    ('%s=%s' % (highlight('NODE_FIELD', field[0]), field[1]) for field in fields)
                ))
                return rv + ')'
            elif isinstance(node, list):
                return '[%s]' % ', '.join(_format(x) for x in node)
            return highlight('LITERAL', repr(node))
        if not isinstance(node, semantix.ast.base.AST):
            raise TypeError('expected semantix.ast.base.AST, got %r' % node.__class__.__name__)
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

        result = '%s%s(\n' % (pad, highlight('NODE_NAME', node.__class__.__name__))

        for field, value in semantix.ast.base.iter_fields(node):
            if isinstance(value, semantix.ast.base.AST):
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

        result += '%s)\n' % pad
        return (False, result)

    return _format(node)[1]
