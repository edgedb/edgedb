##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys
import os
import imp
import importlib.abc
import collections
import types

from semantix.utils.lang.meta import LanguageMeta, DocumentContext


class ImportContext(str):
    def __getitem__(self, key):
        result = super().__getitem__(key)
        return self.__class__.copy(result, self)

    @classmethod
    def copy(cls, name, other):
        return cls(name)

    @classmethod
    def from_parent(cls, name, parent):
        return cls(name)


class Module(types.ModuleType):
    pass


class BaseProxyModule:
    def __init__(self, name, module):
        self.__name__ = name
        self.__wrapped__ = module


class LightProxyModule(BaseProxyModule):
    """Light ProxyModule object, does not keep track of wrapped
    module's attributes, so if there are any references to them in
    the code then it may be broken after reload.
    """

    def __setattr__(self, name, value):
        if name not in ('__name__', '__wrapped__'):
            return setattr(self.__wrapped__, name, value)
        else:
            return object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        if name in ('__name__', '__repr__', '__wrapped__'):
            return object.__getattribute__(self, name)

        wrapped = object.__getattribute__(self, '__wrapped__')
        return getattr(wrapped, name)

    def __repr__(self):
        return '<%s "%s">' % (object.__getattribute__(self, '__class__').__name__, self.__name__)


class Importer(importlib.abc.Finder, importlib.abc.Loader):
    _modules_by_file = {}

    @classmethod
    def get_module_by_filename(cls, filename):
        normpath = os.path.abspath(os.path.realpath(filename))
        modname = cls._modules_by_file.get(normpath)
        if modname:
            return sys.modules.get(modname)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.module_file_map = dict()

    def _locate_module_file(self, fullname, path):
        basename = fullname.rpartition('.')[2]

        for p in path:
            test = os.path.join(p, basename)
            is_package = os.path.isdir(test)
            result = LanguageMeta.recognize_file(test, True, is_package)
            if result:
                return result + (is_package,)

    def find_module(self, fullname, path=None):
        if path:
            result = self._locate_module_file(fullname, path)
            if result:
                self.module_file_map[fullname] = result
                return self

    def load_module(self, fullname):
        language, filename, is_package = self.module_file_map[fullname]

        if language.loader:
            module = language.loader(fullname, filename).load_module(fullname)
        else:
            module = self._load_module(fullname, language, filename, is_package)

        normpath = os.path.abspath(os.path.realpath(filename))
        self.__class__._modules_by_file[normpath] = fullname
        return module

    def _load_module(self, fullname, language, filename, is_package):
        orig_mod = new_mod = sys.modules.get(fullname)
        reload = new_mod is not None
        proxied = language.proxy_module_cls and isinstance(orig_mod, BaseProxyModule)

        if proxied:
            new_mod = orig_mod.__wrapped__

        if not reload:
            new_mod = imp.new_module(fullname)
            sys.modules[fullname] = new_mod
        else:
            for k in new_mod.__odict__.keys():
                del new_mod.__dict__[k]

        new_mod.__file__ = filename
        if is_package:
            new_mod.__path__ = [os.path.dirname(filename)]
            new_mod.__package__ = fullname
        else:
            new_mod.__path__ = None
            new_mod.__package__ = fullname.rpartition('.')[0]

        new_mod.__odict__ = collections.OrderedDict()
        new_mod.__loader__ = self
        new_mod._language_ = language

        context = DocumentContext(module=new_mod, import_context=fullname)

        with open(filename) as stream:
            try:
                attributes = language.load_dict(stream, context=context)

                for attribute_name, attribute_value in attributes:
                    attribute_name = str(attribute_name)
                    new_mod.__odict__[attribute_name] = attribute_value
                    setattr(new_mod, attribute_name, attribute_value)

            except Exception as error:
                if not reload:
                    del sys.modules[fullname]
                raise ImportError('unable to import "%s" (%s: %s)' \
                                  % (fullname, type(error).__name__, error)) from error

        result_mod = new_mod
        if language.proxy_module_cls:
            assert issubclass(language.proxy_module_cls, BaseProxyModule)

            if proxied:
                orig_mod.__wrapped__ = new_mod
                result_mod = orig_mod
            else:
                result_mod = language.proxy_module_cls(fullname, new_mod)

        sys.modules[fullname] = result_mod
        return result_mod


def reload(module):
    if isinstance(module, BaseProxyModule):
        sys.modules[module.__name__] = module.__wrapped__

        new_mod = imp.reload(module.__wrapped__)
        if isinstance(new_mod, BaseProxyModule):
            module.__wrapped__ = new_mod.__wrapped__
        else:
            module.__wrapped__ = new_mod

        sys.modules[module.__name__] = module
        return module

    else:
        return imp.reload(module)
