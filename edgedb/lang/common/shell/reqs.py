##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic import app
from metamagic.exceptions import MetamagicError


class UnsatisfiedRequirementError(MetamagicError):
    pass


class CommandRequirement:
    pass


class ValidApplication(CommandRequirement):
    def __init__(self, args):
        if not app.Application.active:
            raise UnsatisfiedRequirementError('need active Application')
