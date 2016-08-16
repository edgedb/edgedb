##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import derivable
from . import expr
from . import name as sn
from . import named
from . import objects as so
from . import primary
from . import referencing


class IndexSourceCommandContext:
    pass


class IndexSourceCommand(sd.PrototypeCommand):
    def _create_innards(self, schema, context):
        super()._create_innards(schema, context)

        for op in self(SourceIndexCommand):
            op.apply(schema, context=context)

    def _alter_innards(self, schema, context, prototype):
        super()._alter_innards(schema, context, prototype)

        for op in self(SourceIndexCommand):
            op.apply(schema, context=context)

    def _delete_innards(self, schema, context, prototype):
        super()._delete_innards(schema, context, prototype)

        for op in self(SourceIndexCommand):
            op.apply(schema, context=context)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(SourceIndexCommand):
            self._append_subcmd_ast(node, op, context)


class SourceIndexCommandContext(sd.PrototypeCommandContext):
    pass


class SourceIndexCommand(referencing.ReferencedPrototypeCommand):
    context_class = SourceIndexCommandContext
    referrer_conext_class = IndexSourceCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return SourceIndex

    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.prototype_name

        idx_name = SourceIndex.generate_specialized_name(
            subject_name,
            sn.Name(name=astnode.name.name, module=subject_name)
        )

        return sn.Name(name=idx_name, module=subject_name.module)


class CreateSourceIndex(SourceIndexCommand, named.CreateNamedPrototype):
    astnode = qlast.CreateIndexNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.prototype_name

        cmd.update((
            sd.AlterPrototypeProperty(
                property='subject',
                new_value=so.PrototypeRef(prototype_name=subject_name)
            ),
            sd.AlterPrototypeProperty(
                property='expr',
                new_value=expr.ExpressionText(
                    edgeql.generate_source(astnode.expr, pretty=False))
            )
        ))

        return cmd

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        node.name.module = ''

    def _apply_field_ast(self, context, node, op):
        if op.property == 'expr':
            node.expr = op.new_value
        elif op.property == 'is_derived':
            pass
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(context, node, op)


class RenameSourceIndex(SourceIndexCommand, named.RenameNamedPrototype):
    pass


class AlterSourceIndex(SourceIndexCommand, named.AlterNamedPrototype):
    pass


class DeleteSourceIndex(SourceIndexCommand, named.DeleteNamedPrototype):
    astnode = qlast.DropIndexNode


class SourceIndex(derivable.DerivablePrototype):
    _type = 'index'

    subject = so.Field(primary.Prototype)
    expr = so.Field(str, compcoef=0.909)

    delta_driver = sd.DeltaDriver(
        create=CreateSourceIndex,
        alter=AlterSourceIndex,
        delete=DeleteSourceIndex,
        rename=RenameSourceIndex
    )

    def __repr__(self):
        cls = self.__class__
        return '<{}.{} {!r} {!r} at 0x{:x}>'.format(
                cls.__module__, cls.__name__, self.name, self.expr, id(self))

    __str__ = __repr__


class IndexableSubject(referencing.ReferencingPrototype):
    indexes = referencing.RefDict(ref_cls=SourceIndex, compcoef=0.909)

    def add_index(self, index):
        self.add_protoref('indexes', index)

    def del_index(self, index, proto_schema):
        self.del_protoref('indexes', index, proto_schema)
