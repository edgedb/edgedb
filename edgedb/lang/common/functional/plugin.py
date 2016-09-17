##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class PluginError(TypeError):
    pass


class PluginMeta(type):
    """Generic plugin registry.

    A base metaclass used to support pluggable architectures where
    interface implementaitons are loosely coupled and the specific
    implementations to be used are specified in config.

    This metaclass is not meant to be used directly.  A sub-metaclass
    should be used instead as a natural namespace for the specific
    universe of plugins used.

    Example:

    .. code-block:: python

          class MyPluginMeta(PluginMeta):
              pass


          class MyPlugin(metaclass=MyPluginMeta):
              pass


          class MyPluginA(MyPlugin):
              pass


          def my_generic_interface():
              plugin = MyPluginMeta.get_plugin_by_name(
                __name__ + '.' + 'MyPluginA')
    """

    def __new__(mcls, name, bases, dct, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)

        fullname = '{}.{}'.format(dct['__module__'], name)

        try:
            registry = mcls._registry
        except AttributeError:
            registry = mcls._registry = {}

        registry[fullname] = cls

        return cls

    def __init__(cls, name, bases, dct, **kwargs):
        super().__init__(name, bases, dct, **kwargs)

    @classmethod
    def get_plugin_by_name(mcls, name):
        """Return a registered plugin with fully-qualified ``name``.

        :param str name: Fully-qualified class name of the plugin to return'

        :return: Plugin class, or None if no plugin with specified ``name``
                 was defined, or if the plugin is not derived from this
                 metaclass.
        """
        return mcls._registry.get(name)
