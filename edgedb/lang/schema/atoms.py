##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import attributes
from . import basetypes as s_basetypes
from . import constraints
from . import delta as sd
from . import expr
from . import inheriting
from . import name as sn
from . import named
from . import nodes
from . import objects as so
from . import types as s_types


class Atom(nodes.Node, constraints.ConsistencySubject,
           attributes.AttributeSubject):
    _type = 'atom'

    default = so.Field(expr.ExpressionText, default=None,
                       coerce=True, compcoef=0.909)

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
                ptypes = constraint.paramtypes
                if ptypes:
                    for ptype in ptypes:
                        if isinstance(ptype, s_types.Collection):
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
        return s_basetypes.BaseTypeMeta.get_implementation(base_class.name)

    def coerce(self, value, schema):
        base_t = self.get_implementation_type()

        if not isinstance(value, base_t):
            return base_t(value)
        else:
            return value

    def iscompatible(self, other: s_types.Type, schema) -> bool:
        if self.issubclass(other) or other.issubclass(self):
            # Atom compatibility is symmetric, i.e. a superclass instance
            # is compatible with subclasses, as they all share the same
            # fundamental type.
            return True

        # In lieu of schema-level cast support, use the following
        # compatibility map to plug the hole.
        for tn, compat_names in _compatibility_map.items():
            t = schema.get(tn)
            if self.issubclass(t):
                if other.issubclass(
                        tuple(schema.get(c) for c in compat_names)):
                    return True

        return False


_compatibility_map = {
    'std::str': ['std::sequence'],
    'std::int': ['std::float'],
}


class AtomCommandContext(sd.ClassCommandContext,
                         attributes.AttributeSubjectCommandContext,
                         constraints.ConsistencySubjectCommandContext,
                         nodes.NodeCommandContext):
    pass


class AtomCommand(constraints.ConsistencySubjectCommand,
                  attributes.AttributeSubjectCommand,
                  nodes.NodeCommand,
                  schema_metaclass=Atom,
                  context_class=AtomCommandContext):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)
        cmd = cls._handle_view_op(cmd, astnode, context, schema)
        return cmd


class CreateAtom(AtomCommand, inheriting.CreateInheritingClass):
    astnode = qlast.CreateAtom

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        for sub in cmd.get_subcommands(type=sd.AlterClassProperty):
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
    astnode = qlast.AlterAtom


class DeleteAtom(AtomCommand, inheriting.DeleteInheritingClass):
    astnode = qlast.DropAtom
