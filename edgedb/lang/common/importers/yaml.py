import sys
import os
import imp
from importlib import abc

from semantix import lang

class YamlImporter(abc.Finder, abc.Loader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.module_file_map = dict()

    def _locate_module_file(self, fullname):
        file = fullname.replace('.', '/') + '.yml'
        for path in sys.path:
            test = os.path.join(path, file)
            if os.path.exists(test):
                return test

    def find_module(self, fullname, path=None):
        if path:
            filename = self._locate_module_file(fullname)
            if filename:
                self.module_file_map[fullname] = filename
                return self

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]

        filename = self.module_file_map[fullname]

        new_mod = imp.new_module(fullname)
        setattr(new_mod, '__file__', filename)
        sys.modules[fullname] = new_mod

        try:
            attributes = lang.load(filename)
        except Exception as error:
            raise ImportError('unable to import "%s" (%s)' % (fullname, error))

        for attribute_name, attribute_value in attributes:
            if attribute_name:
                setattr(new_mod, attribute_name, attribute_value)

        return new_mod
