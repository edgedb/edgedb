##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import app, SemantixError


class UnsatisfiedRequirementError(SemantixError):
    pass


class CommandRequirement:
    pass


class ValidApplication(CommandRequirement):
    def __init__(self):
        if not app.Application.active:
            raise UnsatisfiedRequirementError('need active Application')
