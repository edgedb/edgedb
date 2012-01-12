##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .meta import LanguageMeta
from .import_ import ImportContext

# Import languages to register them
from semantix.utils.lang import yaml, python, javascript


class SemantixLangLoaderError(Exception):
    pass


def load(filename, context=None):
    (lang, filename) = LanguageMeta.recognize_file(filename)
    if lang:
        with open(filename) as f:
            result = lang.load(f, context)
            for d in result:
                yield d
        return

    raise SemantixLangLoaderError('unable to load file:  %s' % filename)
