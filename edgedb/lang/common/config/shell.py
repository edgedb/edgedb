##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
import textwrap

from semantix.utils import shell
from semantix.utils.io.terminal import Terminal
from semantix.utils.config import config, _Config, cvalue, ConfigError, \
                                  ConfigRequiredValueError, NoDefault, \
                                  ConfigAbstractValueError


class _Renderer:
    term = None
    tab = ' ' * 4
    right_padding = 20

    @classmethod
    def hl(cls, str, style):
        if style == 'key':
            return str
        elif style == 'unbound_key':
            return cls.term.colorstr(str, color='red')
        elif style == 'value':
            return cls.term.colorstr(str, color='cyan', opts=('bold',))
        if style == 'default_value':
            return str
        elif style == 'keyword':
            return cls.term.colorstr(str, color='black', opts=('bold',))
        elif style == 'error':
            return cls.term.colorstr(str, color='white', bgcolor='red')
        elif style == 'doc':
            return cls.term.colorstr(str, color='blue', opts=('bold',))
        elif style == 'sep':
            return cls.term.colorstr(str, color='black', opts=('bold',))
        elif style == 'type':
            return cls.term.colorstr(str, color='black', opts=('bold',))
        elif style == 'path':
            return cls.term.colorstr(str, color='red')
        else:
            raise NotImplemented

    @classmethod
    def render(cls, node, term, *, filter=None, verbose=False, width=80):
        cls.term = term
        filter = filter or (lambda item: True)
        cls._render(node, filter_func=filter, verbose=verbose, width=width)

    @classmethod
    def _render_cvalue(cls, name, value, *, level=0, verbose=False, width):
        assert isinstance(value, cvalue)

        print(cls.tab * level, end='')
        line_len = len(cls.tab * level)
        line2_len = 0

        if verbose:
            if isinstance(value._owner, types.FunctionType):
                print(cls.hl('argument ', 'keyword'), end='')
                line_len += len('argument ')
            else:
                print(cls.hl('property ', 'keyword'), end='')
                line_len += len('property ')

        print(name, end='')
        line_len += len(name)
        space = '\n' + ' ' * (line_len + len('-> '))

        try:
            val = value._get_value()

        except ConfigRequiredValueError:
            print(cls.hl(' -> ', 'sep'), end='')
            print(cls.hl('<required, but not set>', 'error'), end='')

        except ConfigAbstractValueError:
            print(cls.hl(' -> ', 'sep'), end='')
            print(cls.hl('<abstract>', 'error'), end='')

        except (TypeError, ConfigError) as ex:
            print(end=' ')
            print(cls.hl('ERROR DURING VALUE CALCULATION', 'error'), end='')

        else:
            if val != value.default:
                val_repr = repr(val)
                print(cls.hl(' -> ', 'sep'), end='')
                print(cls.hl(val_repr, 'value'), end='')
                val_len = 4 + len(val_repr)

                if value.default is not NoDefault:
                    if value.doc:
                        if 20 + len(val_repr) + val_len + cls.right_padding + line_len > width:
                            print(space, end='')
                            line2_len = len(space)

                    default_repr = ' (default: %s)' % repr(value.default)
                    print(default_repr, end='')
                    if line2_len:
                        line2_len += len(default_repr)
                    else:
                        line_len += len(default_repr)
                else:
                    line_len += val_len

            else:
                val_repr = repr(value.default)
                print(cls.hl(' -> ', 'sep'), end='')
                print(cls.hl(val_repr, 'default_value'), end='')
                val_len = 4 + len(val_repr)
                line_len += val_len

            if value.type and isinstance(value.type, type):
                print(cls.hl(' <%s>' % value.type.__name__, 'type'), end='')

        if value.doc:
            wrap_width, wrap_pad = 0, 0

            if (line2_len and line2_len + len(value.doc) + cls.right_padding > width) or \
                            (line_len + len(value.doc) + cls.right_padding + 10 > width):

                wrap_width = width - len(space) - cls.right_padding
                if wrap_width < 30:
                    wrap_width = 30
                wrap_pad = len(space)

            if not wrap_width:
                print('', cls.hl('"%s"' % value.doc, 'doc'), end='')
            else:
                print()
                print(cls.hl('\n'.join(textwrap.wrap('"%s"' % value.doc,
                                              wrap_width+wrap_pad,
                                              initial_indent=' ' * wrap_pad,
                                              subsequent_indent= ' ' * wrap_pad,
                                              drop_whitespace=False)), 'doc'))
        print()

    @classmethod
    def _render(cls, node, *, level=0, filter_func=None, prefix='', verbose=False, width):
        assert isinstance(node, _Config)

        filtered = list(filter(filter_func, node))

        if len(filtered) == 1 and isinstance(filtered[0][1], _Config) \
                                                                and not filtered[0][1]._bound_to:

            prefix = prefix + filtered[0][0] + '.' if prefix else filtered[0][0] + '.'
            cls._render(filtered[0][1], level=level, filter_func=filter_func,
                        prefix=prefix, verbose=verbose, width=width)

        else:
            if prefix:
                print(cls.tab * level, end='')
                print(prefix[:-1] + ':')
                level += 1

            for key, item in filtered:
                if isinstance(item, _Config) or isinstance(item, cvalue):
                    if item._bound_to:
                        if isinstance(item._bound_to, type):
                            if isinstance(item, _Config):
                                print(cls.tab * level, end='')
                                if verbose:
                                    print(cls.hl('class ', 'keyword'), end='')
                                print(cls.hl(key + ':', 'key'))

                            elif isinstance(item, cvalue):
                                cls._render_cvalue(key, item, level=level, verbose=verbose,
                                                   width=width)

                        elif isinstance(item._bound_to, types.FunctionType):
                            if isinstance(item, _Config):
                                print(cls.tab * level, end='')
                                if verbose:
                                    print(cls.hl('function ', 'keyword'), end='')
                                print(cls.hl(key + ':', 'key'))

                            elif isinstance(item, cvalue):
                                cls._render_cvalue(key, item, level=level, verbose=verbose,
                                                   width=width)

                    else:
                        print(cls.tab * level, end='')
                        print(key + ':')

                else:
                    print(cls.tab * level, end='')
                    print(cls.hl('unbound ', 'keyword'), end='')
                    print(cls.hl(key + ':', 'unbound_key'), end=' ')
                    print(repr(item[0]))

                    print(cls.tab * level, end='')
                    print('        ', end='')
                    print(cls.hl(item[1], 'path'))

                if isinstance(item, _Config):
                    cls._render(item, level=level+1, filter_func=filter_func, verbose=verbose,
                                width=width)


class ConfigList(shell.Command, name='list'):
    def get_parser(self, subparsers):
        parser = super().get_parser(subparsers)

        parser.add_argument('--verbose', action='store_true', default=False)
        return parser

    def __call__(self, args):
        term = Terminal(colors=args.color)
        _, width = term.size
        _Renderer.render(config, term, verbose=args.verbose, width=width)


class ConfigCommands(shell.CommandGroup,
                   name='config',
                   expose=True,
                   commands=(ConfigList,)):
    pass
