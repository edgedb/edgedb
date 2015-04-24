##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

from importkit import yaml
from importkit import context as lang_context
from metamagic.utils.slots import SlotsMeta

from ..base import ConfigRootNode
from .schema import Schema # <- this must be imported in this module.


class _YamlObjectMeta(yaml.ObjectMeta, SlotsMeta):
    pass


class _YamlObject(yaml.Object, metaclass=_YamlObjectMeta):
    __slots__ = ()

    def __sx_setstate__(self, data):
        object.__setattr__(self, '__node_yaml_data__', data)


class ConfigYAMLAdapter(_YamlObject, adapts=ConfigRootNode):
    __slots__ = '__node_yaml_data__',

    @classmethod
    def traverse(cls, root, obj, name=''):
        if isinstance(obj, yaml.Object):
            if isinstance(obj.__node_yaml_data__, dict):
                for key in obj.__node_yaml_data__:
                    cls.traverse(root, obj.__node_yaml_data__[key],
                                 (name + '.' + key) if name else key)
            else:
                context = lang_context.SourceContext.from_object(obj)
                ConfigRootNode._set_value(root, name, obj.__node_yaml_data__, str(context))

    def __sx_setstate__(self, data):
        super().__sx_setstate__(data)
        ConfigRootNode.__init__(self, 'config')

        self.__class__.traverse(self, self)

        context = lang_context.SourceContext.from_object(self)
        file = self.__node_ctx__['__file__'] = context.document.module.__file__
        dir = self.__node_ctx__['__dir__'] = os.path.dirname(file) + os.path.sep
        self.__node_ctx__['__realdir__'] = os.path.realpath(dir) + os.path.sep
