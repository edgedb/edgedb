##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import types

from edgedb.lang.common.functional import decorate


class MenuMeta(type):
    @classmethod
    def __prepare__(metacls, name, bases):
        return collections.OrderedDict()

    def __init__(cls, name, bases, dct):
        super(MenuMeta, cls).__init__(name, bases, dct)
        options = collections.OrderedDict()

        for c in bases:
            opts = getattr(c, 'options', None)
            if opts:
                options.update(opts)

        for v in dct.values():
            option = getattr(v, 'menuoption', None)
            if option:
                options[option.title] = option

        setattr(cls, 'options', options)


class Option:
    def __init__(self, title, hotkey, callback):
        self.title = title
        self.hotkey = hotkey
        self.callback = callback

    def __str__(self):
        return "<%s.%s '%s'>" % (self.__class__.__module__, self.__class__.__name__, self.title)

    __repr__ = __str__


def option(title=None, hotkey=None):
    def wrap(fn):
        nonlocal title
        if title is None:
            title = fn.__name__.replace('_', ' ')
        fn.menuoption = Option(title, hotkey, fn)
        return fn

    return wrap


class Menu(metaclass=MenuMeta):
    def __init__(self, terminal, prompt='> '):
        self.prompt = terminal.colorstr(prompt, 'blue', ('bold',))
        self.term = terminal

    def __call__(self):
        while True:
            self.print_menu()

            option = None
            opt = input(self.prompt)

            try:
                index = int(opt)

                if 0 < index <= len(self.__class__.options):
                    option = list(self.__class__.options.values())[index - 1]
            except ValueError:
                for title, o in self.__class__.options.items():
                    if title.startswith(opt):
                        option = o
                        break

            if option:
                option.callback(self)
            else:
                print('Unrecognized input: %s' % opt)

    def print_menu(self):
        print(self.term.colorstr('*** Commands ***', 'white', ('bold',)))
        maxname = max(max(len(title) for title in self.__class__.options), 15)
        for i, item in enumerate(self.__class__.options.values()):
            i += 1
            end = '' if i % 4 else '\n'
            print('  {0}. {1:{width}}'.format(i, item.title, width=maxname), end=end)
        if i % 4:
            print()
