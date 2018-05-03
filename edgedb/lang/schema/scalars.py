##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

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


class ScalarType(nodes.Node, constraints.ConsistencySubject,
                 attributes.AttributeSubject):
    _type = 'ScalarType'

    default = so.Field(expr.ExpressionText, default=None,
                       coerce=True, compcoef=0.909)

    def _get_deps(self):
        deps = super()._get_deps()

        if self.constraints:
            N = sn.Name

            # Add dependency on all built-in scalars unconditionally
            deps.add(N(module='std', name='str'))
            deps.add(N(module='std', name='bytes'))
            deps.add(N(module='std', name='int16'))
            deps.add(N(module='std', name='int32'))
            deps.add(N(module='std', name='int64'))
            deps.add(N(module='std', name='float32'))
            deps.add(N(module='std', name='float64'))
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
                                if isinstance(subtype, so.ObjectRef):
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
        """Get the underlying Python type that is used to implement this ScalarType.
        """
        base_class = self.get_topmost_concrete_base()
        return s_basetypes.BaseTypeMeta.get_implementation(base_class.name)

    def coerce(self, value, schema):
        base_t = self.get_implementation_type()

        if not isinstance(value, base_t):
            return base_t(value)
        else:
            return value

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        if self.issubclass(other) or other.issubclass(self):
            # ScalarType compatibility is symmetric, i.e. a superclass instance
            # is compatible with subclasses, as they all share the same
            # fundamental type.
            return True

        # In lieu of schema-level cast support, use the
        # hardcoded cast map to plug the hole.
        for tn, castable_to in _full_implicit_cast_map.items():
            t = schema.get(tn)
            if self.issubclass(t):
                if other.issubclass(
                        tuple(schema.get(c) for c in castable_to)):
                    return True
                else:
                    return False

        return False


# Target type : source types that can implicitly cast into target
_implicit_cast_map = {
    'std::int32': ['std::int16'],
    'std::int64': ['std::int32'],
    'std::float64': ['std::float32', 'std::int64'],
    'std::decimal': ['std::float64']
}

# Expanded cast map: source type: target_type
_full_implicit_cast_map = collections.defaultdict(set)


def _build_implicit_cast_map():
    for target, sources in _implicit_cast_map.items():
        for source in sources:
            _full_implicit_cast_map[source].add(target)

    for source, targets in _full_implicit_cast_map.items():
        seen = set()

        while targets - seen:
            for target in list(targets):
                transitive = _full_implicit_cast_map.get(target)
                if transitive:
                    targets.update(transitive)
                seen.add(target)


_build_implicit_cast_map()


class ScalarTypeCommandContext(sd.ObjectCommandContext,
                               attributes.AttributeSubjectCommandContext,
                               constraints.ConsistencySubjectCommandContext,
                               nodes.NodeCommandContext):
    pass


class ScalarTypeCommand(constraints.ConsistencySubjectCommand,
                        attributes.AttributeSubjectCommand,
                        nodes.NodeCommand,
                        schema_metaclass=ScalarType,
                        context_class=ScalarTypeCommandContext):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)
        cmd = cls._handle_view_op(cmd, astnode, context, schema)
        return cmd


class CreateScalarType(ScalarTypeCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateScalarType

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        for sub in cmd.get_subcommands(type=sd.AlterObjectProperty):
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


class RenameScalarType(ScalarTypeCommand, named.RenameNamedObject):
    pass


class RebaseScalarType(ScalarTypeCommand, inheriting.RebaseNamedObject):
    pass


class AlterScalarType(ScalarTypeCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterScalarType


class DeleteScalarType(ScalarTypeCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropScalarType
