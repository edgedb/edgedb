##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import caosql
from edgedb.lang.caosql import ast as qlast

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


class SourceIndexCommandContext(sd.PrototypeCommandContext):
    pass


class SourceIndexCommand(sd.PrototypeCommand):
    context_class = SourceIndexCommandContext

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
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

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
                    caosql.generate_source(astnode.expr, pretty=False))
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

    def apply(self, schema, context):
        source = context.get(IndexSourceCommandContext)
        assert source, "SourceIndex commands must be run in Source context"
        index = named.CreateNamedPrototype.apply(self, schema, context)
        index.subject = source.proto
        source.proto.add_index(index)
        return index


class RenameSourceIndex(SourceIndexCommand, named.RenameNamedPrototype):
    def apply(self, schema, context):
        index = super().apply(schema, context)

        subject_ctx = context.get(IndexSourceCommandContext)
        msg = "Index commands must be run in SourceSubject context"
        assert subject_ctx, msg

        subject = subject_ctx.proto

        norm = SourceIndex.normalize_name
        cur_name = norm(self.prototype_name)
        new_name = norm(self.new_name)

        local = subject.local_indexes.pop(cur_name, None)
        if local:
            subject.local_indexes[new_name] = local

        inherited = subject.indexes.pop(cur_name, None)
        if inherited is not None:
            subject.indexes[new_name] = inherited

        return index


class AlterSourceIndex(SourceIndexCommand, named.AlterNamedPrototype):
    def apply(self, schema, context=None):
        context = context or sd.CommandContext()
        with context(SourceIndexCommandContext(self, None)):
            index = super().apply(schema, context)

        return index


class DeleteSourceIndex(SourceIndexCommand, named.DeleteNamedPrototype):
    astnode = qlast.DropIndexNode

    def apply(self, schema, context):
        source = context.get(IndexSourceCommandContext)
        assert source, "SourceIndex commands must be run in Source context"
        index = super().apply(schema, context)

        source.proto.del_index(index, schema)

        return index


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
