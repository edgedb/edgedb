##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic import app, MetamagicError


class UnsatisfiedRequirementError(MetamagicError):
    pass


class CommandRequirement:
    pass


class ValidApplication(CommandRequirement):
    def __init__(self):
        if not app.Application.active:
            raise UnsatisfiedRequirementError('need active Application')
