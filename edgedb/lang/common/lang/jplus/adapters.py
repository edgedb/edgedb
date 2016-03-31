##
# Copyright (c) 2013-2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Base JPlus adapters library"""


import sys
import textwrap

from metamagic import json
from importkit import runtimes as lang_runtimes
from metamagic.utils.datastructures import OrderedSet

from jplus.compiler.environment import metamagic as jplus


class BaseClassAdapter(jplus.JPlusWebRuntimeAdapter):
    # NB: We want this config property to be explicit.
    # We could also use '__sx_adaptee__', but then it's hard
    # to derive adapters
    base_class = None

    def collect_candidate_imports(self):
        cls = self.attr_value

        imps = super().collect_candidate_imports()

        for base in cls.__bases__:
            if (issubclass(base, self.base_class) and
                    base.__module__ != self.module.__name__):
                basemod = sys.modules[base.__module__]
                imps.add(basemod)

        return imps

    def _build_class_bases_template(self):
        bases = self._get_class_bases()
        return ', '.join(bases)

    def _build_class_name_template(self):
        cls = self.attr_value
        return cls.__name__

    def _build_class_dict_template(self):
        dct = self._get_class_dict()
        dct = '\n'.join('{} = {}'.format(k, v) for k, v in dct.items())
        return textwrap.indent(dct, '    ')

    def _get_class_bases(self):
        cls = self.attr_value

        bases = []
        for base in cls.__bases__:
            if issubclass(base, self.base_class):
                if base.__module__ != self.module.__name__:
                    lang_runtimes.load_module_for_runtime(
                        base.__module__, self.runtime)
                    base_mod = sys.modules[base.__module__]
                    is_compat = lang_runtimes.module_runtimes_compatible(
                                    self.module, base_mod)
                    if is_compat:
                        bases.append(
                            '{}.{}'.format(base.__module__, base.__name__))
                else:
                    bases.append(base.__name__)

        return bases

    def _get_class_dict(self):
        raise NotImplementedError

    def _get_class_template(self):
        return '''
            class {name}({bases}) {{
                {dct}
            }}
            export {name}
        '''

    def get_jplus_source(self):
        name = self._build_class_name_template()
        bases = self._build_class_bases_template()
        dct = self._build_class_dict_template()
        template = textwrap.dedent(self._get_class_template())

        return template.format(name=name, bases=bases, dct=dct)
