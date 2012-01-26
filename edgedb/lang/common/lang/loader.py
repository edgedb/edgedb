##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import io

from semantix.utils.lang.import_ import loader
from semantix.utils.lang.import_ import module as module_types

from .context import DocumentContext


class LanguageLoader:
    def __init__(self, fullname, filename, language):
        super().__init__(fullname, filename)
        self._language = language
        self._context = fullname

    def get_proxy_module_class(self):
        return self._language.proxy_module_cls

    def code_from_source(self, module, source_bytes):
        modinfo = module_types.ModuleInfo(module)
        context = DocumentContext(module=modinfo, import_context=module.__name__)

        stream = io.BytesIO(source_bytes)

        try:
            code = self._language.load_code(stream, context=context)
        except NotImplementedError:
            raise
        except Exception as error:
            raise ImportError('unable to import "%s" (%s: %s)' \
                              % (module.__name__, type(error).__name__, error)) from error

        return code

    def _execute(self, module, data, method):
        try:
            modinfo = module_types.ModuleInfo(module)
            context = DocumentContext(module=modinfo, import_context=self._context)
            attributes = getattr(self._language, method)(data, context=context)
            self.set_module_attributes(module, attributes)

        except Exception as error:
            raise ImportError('unable to import "%s" (%s: %s)' \
                              % (module.__name__, type(error).__name__, error)) from error

    def set_module_attributes(self, module, attributes):
        module.__odict__ = collections.OrderedDict()

        for attribute_name, attribute_value in attributes:
            attribute_name = str(attribute_name)
            module.__odict__[attribute_name] = attribute_value
            setattr(module, attribute_name, attribute_value)

    def execute_module_code(self, module, code):
        self._execute(module, code, 'execute_code')

    def execute_module(self, module):
        source = self.get_source_bytes(module.__name__)
        stream = io.BytesIO(source)
        self._execute(module, stream, 'load_dict')

    def invalidate_module(self, module):
        for k in module.__odict__.keys():
            try:
                del module.__dict__[k]
            except KeyError:
                pass


class LanguageSourceFileLoader(LanguageLoader, loader.SourceFileLoader):
    def load_module(self, fullname):
        module = self._load_module(fullname)
        module.__language__ = self._language
        return module
