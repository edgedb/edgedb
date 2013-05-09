##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang import meta as lang_meta


class YAMLError(lang_meta.LanguageError):
    def __init__(self, msg, *, details=None, hint=None, context=None):
        super().__init__(msg, details=details, hint=hint, context=context)


class YAMLCompositionError(YAMLError):
    pass
