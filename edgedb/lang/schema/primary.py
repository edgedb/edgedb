##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import types

from metamagic.utils.nlang import morphology
from metamagic.utils.functional import get_safe_attrname

from . import name as sn
from . import objects as so
from . import referencing
from . import schema as s_schema
from . import types as s_types


class Prototype(referencing.ReferencingPrototype):
    title = so.Field(morphology.WordCombination, default=None, compcoef=0.909)
    description = so.Field(str, default=None, compcoef=0.909)

    def get_type_property(self, name, schema):
        from . import lproperties as lprops

        if name == 'id':
            atom_name = 'metamagic.caos.builtins.int'
        else:
            atom_name = 'metamagic.caos.builtins.str'

        target = schema.get(atom_name)

        return lprops.TypeProperty(source=self, target=target,
                                   name=sn.Name(module='type', name=name))
