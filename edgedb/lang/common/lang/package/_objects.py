##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from metamagic.utils import glob
from metamagic.utils.lang import context as lang_context
from metamagic.utils.lang import yaml


class Package(yaml.Object):
    def __sx_setstate__(self, data):
        tag_mapping = data.get('tags', {})
        tags = collections.OrderedDict()

        context = lang_context.SourceContext.from_object(self)

        for pattern, tag_names in tag_mapping.items():
            if not isinstance(tag_names, list):
               tag_names = [tag_names]

            modpattern = glob.ModuleGlobPattern(pattern)
            tags[modpattern] = [context.resolve_name(tagname) for tagname in tag_names]

        self.tags = tags

    def items(self):
        yield ('__mm_track_dependencies__', True)
        yield ('__mm_package_tagmap__', self.tags)

        context = lang_context.SourceContext.from_object(self)
        yield from context.document.namespace.items()
