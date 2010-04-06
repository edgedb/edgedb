##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml


class Representer(yaml.representer.Representer):
    _ignore_aliases = set()

    def ignore_aliases(self, data):
        return super().ignore_aliases(data) or \
               isinstance(data, tuple(self.__class__._ignore_aliases))

    @classmethod
    def add_ignore_aliases(cls, type):
        cls._ignore_aliases.add(type)


class Dumper(yaml.emitter.Emitter, yaml.serializer.Serializer, Representer, yaml.resolver.Resolver):
    def __init__(self, stream,
                 default_style=None, default_flow_style=None,
                 canonical=None, indent=None, width=None,
                 allow_unicode=None, line_break=None,
                 encoding=None, explicit_start=None, explicit_end=None,
                 version=None, tags=None):

        yaml.emitter.Emitter.__init__(self, stream, canonical=canonical,
                                      indent=indent, width=width,
                                      allow_unicode=allow_unicode, line_break=line_break)
        yaml.serializer.Serializer.__init__(self, encoding=encoding,
                                            explicit_start=explicit_start,
                                            explicit_end=explicit_end,
                                            version=version, tags=tags)
        Representer.__init__(self, default_style=default_style,
                             default_flow_style=default_flow_style)
        yaml.resolver.Resolver.__init__(self)

