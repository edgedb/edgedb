##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types

from semantix.utils import shell
from semantix.utils.io.terminal import Terminal
from semantix.utils.config import config, _Config, cvalue, ConfigError, \
                                  ConfigRequiredValueError, NoDefault


class _Renderer:
    term = None
    tab = ' ' * 4

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
    def render(cls, node, term, *, filter=None, verbose=False):
        cls.term = term
        filter = filter or (lambda item: True)
        cls._render(node, filter_func=filter, verbose=verbose)

    @classmethod
    def _render_cvalue(cls, name, value, *, level=0, verbose=False):
        assert isinstance(value, cvalue)

        print(cls.tab * level, end='')

        if verbose:
            if isinstance(value._owner, types.FunctionType):
                print(cls.hl('argument ', 'keyword'), end='')
            else:
                print(cls.hl('property ', 'keyword'), end='')

        print(name, end='')

        try:
            val = value._get_value()
        except ConfigRequiredValueError:
            print(cls.hl(' -> ', 'sep'), end='')
            print(cls.hl('<required, but not set>', 'error'), end='')
        except (TypeError, ConfigError):
            print(end=' ')
            print(cls.hl('ERROR DURING VALUE CALCULATION', 'error'), end='')
        else:
            if val != value.default:
                print(cls.hl(' -> ', 'sep'), end='')
                print(cls.hl(repr(val), 'value'), end='')
                if value.default is not NoDefault:
                    print(' (default: %r)' % value.default, end='')

            else:
                print(cls.hl(' -> ', 'sep'), end='')
                print(cls.hl('%r' % value.default, 'default_value'), end='')

            if value.type:
                print(cls.hl(' <%s>' % value.type.__name__, 'type'), end='')

        if value.doc:
            print('', cls.hl('"%s"' % value.doc, 'doc'), end='')

        print()

    @classmethod
    def _render(cls, node, *, level=0, filter_func=None, prefix='', verbose=False):
        assert isinstance(node, _Config)

        filtered = list(filter(filter_func, node))

        if len(filtered) == 1 and isinstance(filtered[0][1], _Config) \
                                                                and not filtered[0][1]._bound_to:

            prefix = prefix + filtered[0][0] + '.' if prefix else filtered[0][0] + '.'
            cls._render(filtered[0][1], level=level, filter_func=filter_func,
                        prefix=prefix, verbose=verbose)

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
                                cls._render_cvalue(key, item, level=level, verbose=verbose)

                        elif isinstance(item._bound_to, types.FunctionType):
                            if isinstance(item, _Config):
                                print(cls.tab * level, end='')
                                if verbose:
                                    print(cls.hl('function ', 'keyword'), end='')
                                print(cls.hl(key + ':', 'key'))

                            elif isinstance(item, cvalue):
                                cls._render_cvalue(key, item, level=level, verbose=verbose)

                    else:
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
                    cls._render(item, level=level+1, filter_func=filter_func, verbose=verbose)


class ConfigList(shell.Command, name='list'):
    def get_parser(self, subparsers):
        parser = super().get_parser(subparsers)

        parser.add_argument('--verbose', action='store_true', default=False)
        return parser

    def __call__(self, args):
        term = Terminal(colors=args.color)
        _Renderer.render(config, term, verbose=args.verbose)


class ConfigCommands(shell.CommandGroup,
                   name='config',
                   expose=True,
                   commands=(ConfigList,)):
    pass
