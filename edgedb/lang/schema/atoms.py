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


class AtomCommandContext(sd.PrototypeCommandContext,
                         attributes.AttributeSubjectCommandContext,
                         constraints.ConsistencySubjectCommandContext):
    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if (name == 'proto' and value is not None
                and value.__class__.__name__ != 'Atom'):
            assert False, value


class AtomCommand(sd.PrototypeCommand):
    context_class = AtomCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Atom


class CreateAtom(named.CreateNamedPrototype, AtomCommand):
    astnode = qlast.CreateAtomNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        for sub in cmd(sd.AlterPrototypeProperty):
            if sub.property == 'default':
                sub.new_value = [sub.new_value]

        if astnode.is_abstract:
            cmd.add(sd.AlterPrototypeProperty(
                property='is_abstract',
                new_value=True
            ))

        if astnode.is_final:
            cmd.add(sd.AlterPrototypeProperty(
                property='is_final',
                new_value=True
            ))

        return cmd

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(attributes.AttributeValueCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'default':
            if op.new_value:
                op.new_value = op.new_value[0]
                super()._apply_field_ast(context, node, op)
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        result = super().apply(schema, context)

        with context(AtomCommandContext(self, result)):
            result.acquire_ancestor_inheritance(schema)

            for op in self(attributes.AttributeValueCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

        return result


class RenameAtom(named.RenameNamedPrototype, AtomCommand):
    pass


class RebaseAtom(inheriting.RebaseNamedPrototype, AtomCommand):
    pass


class AlterAtom(named.AlterNamedPrototype, AtomCommand):
    astnode = qlast.AlterAtomNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(attributes.AttributeValueCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        with context(AtomCommandContext(self, None)):
            atom = super().apply(schema, context)

            for op in self(attributes.AttributeValueCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context)

            for op in self(inheriting.RebaseNamedPrototype):
                op.apply(schema, context)

        return atom


class DeleteAtom(named.DeleteNamedPrototype, AtomCommand):
    astnode = qlast.DropAtomNode

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()
        atom = super().apply(schema, context)

        with context(AtomCommandContext(self, atom)):
            for op in self(attributes.AttributeValueCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context)

        return atom


class Atom(primary.Prototype, constraints.ConsistencySubject,
           attributes.AttributeSubject, so.ProtoNode):
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
                                    if isinstance(subtype, so.PrototypeRef):
                                        if subtype.prototype_name != self.name:
                                            deps.add(subtype.prototype_name)
                                    else:
                                        deps.add(subtype.name)

        return deps

    @hybridmethod
    def copy(scope, obj=None):
        if isinstance(scope, type):
            cls = scope
        else:
            obj = scope
            cls = obj.__class__

        result = super(Atom, scope).copy(obj)
        result.default = obj.default
        return result

    def get_implementation_type(self):
        """Get the underlying Python type that is used to implement this Atom.
        """
        base_proto = self.get_topmost_base()
        return s_types.BaseTypeMeta.get_implementation(base_proto.name)

    def coerce(self, value, schema):
        base_t = self.get_implementation_type()

        if not isinstance(value, base_t):
            return base_t(value)
        else:
            return value
