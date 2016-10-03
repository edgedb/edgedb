##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.functional import hybridmethod

from edgedb.lang.edgeql import ast as qlast

from . import attributes
from . import constraints
from . import delta as sd
from . import expr
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import primary
from . import types as s_types


class AtomCommandContext(sd.ClassCommandContext,
                         attributes.AttributeSubjectCommandContext,
                         constraints.ConsistencySubjectCommandContext):
    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if (name == 'scls' and value is not None
                and value.__class__.__name__ != 'Atom'):
            assert False, value


class AtomCommand(constraints.ConsistencySubjectCommand,
                  attributes.AttributeSubjectCommand):
    context_class = AtomCommandContext

    @classmethod
    def _get_metaclass(cls):
        return Atom


class CreateAtom(AtomCommand, inheriting.CreateInheritingClass):
    astnode = qlast.CreateAtomNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        for sub in cmd(sd.AlterClassProperty):
            if sub.property == 'default':
                sub.new_value = [sub.new_value]

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'default':
            if op.new_value:
                op.new_value = op.new_value[0]
                super()._apply_field_ast(context, node, op)
        else:
            super()._apply_field_ast(context, node, op)


class RenameAtom(AtomCommand, named.RenameNamedClass):
    pass


class RebaseAtom(AtomCommand, inheriting.RebaseNamedClass):
    pass


class AlterAtom(AtomCommand, inheriting.AlterInheritingClass):
    astnode = qlast.AlterAtomNode


class DeleteAtom(AtomCommand, inheriting.DeleteInheritingClass):
    astnode = qlast.DropAtomNode


class Atom(primary.PrimaryClass, constraints.ConsistencySubject,
           attributes.AttributeSubject, so.NodeClass):
    _type = 'atom'

    default = so.Field(expr.ExpressionText, default=None,
                       coerce=True, compcoef=0.909)

    delta_driver = sd.DeltaDriver(
        create=CreateAtom,
        alter=AlterAtom,
        rebase=RebaseAtom,
        rename=RenameAtom,
        delete=DeleteAtom
    )

    def _get_deps(self):
        deps = super()._get_deps()

        if self.constraints:
            N = sn.Name

            # Add dependency on all built-in atoms unconditionally
            deps.add(N(module='std', name='str'))
            deps.add(N(module='std', name='bytes'))
            deps.add(N(module='std', name='int'))
            deps.add(N(module='std', name='float'))
            deps.add(N(module='std', name='decimal'))
            deps.add(N(module='std', name='bool'))
            deps.add(N(module='std', name='uuid'))

            for constraint in self.constraints.values():
                for ptypes in (constraint.paramtypes,
                               constraint.inferredparamtypes):
                    if ptypes:
                        for ptype in ptypes.values():
                            if isinstance(ptype, so.Collection):
                                subtypes = ptype.get_subtypes()
                            else:
                                subtypes = [ptype]

                            for subtype in subtypes:
                                if subtype is not self:
                                    if isinstance(subtype, so.ClassRef):
                                        if subtype.classname != self.name:
                                            deps.add(subtype.classname)
                                    else:
                                        deps.add(subtype.name)

        return deps

    def copy(self):
        result = super().copy()
        result.default = self.default
        return result

    def get_implementation_type(self):
        """Get the underlying Python type that is used to implement this Atom.
        """
        base_class = self.get_topmost_base()
        return s_types.BaseTypeMeta.get_implementation(base_class.name)

    def coerce(self, value, schema):
        base_t = self.get_implementation_type()

        if not isinstance(value, base_t):
            return base_t(value)
        else:
            return value
