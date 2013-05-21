##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Base JavaScript adapters library"""


import textwrap

from metamagic import json
from metamagic.utils.lang import runtimes as lang_runtimes
from metamagic.utils.datastructures import OrderedSet
from . import JavaScriptRuntimeAdapter


class BaseClassAdapter(JavaScriptRuntimeAdapter):
    # NB: We want this config property to be explicit.  We could also use '__sx_adaptee__',
    # but then it's hard to derive adapters
    base_class = None

    def get_dependencies(self):
        cls = self.attr_value

        # We don't call `super().get_dependencies()` here, as we want strict
        # control over what we depend on
        deps = OrderedSet()

        for base in cls.__bases__:
            if issubclass(base, self.base_class):
                basemod = lang_runtimes.load_module_for_runtime(base.__module__, self.runtime)
                deps.add(basemod)

        return deps

    def _build_class_bases_template(self):
        bases = self._get_class_bases()
        return '[{}]'.format(', '.join(bases))

    def _build_class_name_template(self):
        cls = self.attr_value
        return json.dumps('{}.{}'.format(cls.__module__, cls.__name__))

    def _build_class_dict_template(self):
        return json.dumps(self._get_class_dict())

    def _get_class_bases(self):
        cls = self.attr_value

        bases = []
        for base in cls.__bases__:
            if issubclass(base, self.base_class):
                bases.append('{}.{}'.format(base.__module__, base.__name__))

        return bases

    def _get_class_dict(self):
        raise NotImplementedError

    def _get_class_template(self):
        raise NotImplementedError

    def get_source(self):
        name = self._build_class_name_template()
        bases = self._build_class_bases_template()
        dct = self._build_class_dict_template()
        template = textwrap.dedent(self._get_class_template())

        return template.format(name=name, bases=bases, dct=dct)
