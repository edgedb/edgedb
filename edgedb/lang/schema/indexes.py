##
# Copyright (c) 2008-present MagicStack Inc.
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


class IndexSourceCommand(named.NamedClassCommand):
    def _create_innards(self, schema, context):
        super()._create_innards(schema, context)

        for op in self.get_subcommands(type=SourceIndexCommand):
            op.apply(schema, context=context)

    def _alter_innards(self, schema, context, scls):
        super()._alter_innards(schema, context, scls)

        for op in self.get_subcommands(type=SourceIndexCommand):
            op.apply(schema, context=context)

    def _delete_innards(self, schema, context, scls):
        super()._delete_innards(schema, context, scls)

        for op in self.get_subcommands(type=SourceIndexCommand):
            op.apply(schema, context=context)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self.get_subcommands(type=SourceIndexCommand):
            self._append_subcmd_ast(node, op, context)


class SourceIndexCommandContext(sd.ClassCommandContext):
    pass


class SourceIndexCommand(referencing.ReferencedClassCommand):
    context_class = SourceIndexCommandContext
    referrer_context_class = IndexSourceCommandContext

    @classmethod
    def _get_metaclass(cls):
        return SourceIndex

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        idx_name = SourceIndex.get_specialized_name(
            sn.Name(name=astnode.name.name, module=subject_name),
            subject_name
        )

        return sn.Name(name=idx_name, module=subject_name.module)

    def _create_begin(self, schema, context):
        return derivable.DerivableClassCommand._create_begin(
            self, schema, context)


class CreateSourceIndex(SourceIndexCommand, named.CreateNamedClass):
    astnode = qlast.CreateIndex

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        cmd.update((
            sd.AlterClassProperty(
                property='subject',
                new_value=so.ClassRef(classname=subject_name)
            ),
            sd.AlterClassProperty(
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


class RenameSourceIndex(SourceIndexCommand, named.RenameNamedClass):
    pass


class AlterSourceIndex(SourceIndexCommand, named.AlterNamedClass):
    pass


class DeleteSourceIndex(SourceIndexCommand, named.DeleteNamedClass):
    astnode = qlast.DropIndex


class SourceIndex(derivable.DerivableClass):
    _type = 'index'

    subject = so.Field(primary.PrimaryClass)
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


class IndexableSubject(referencing.ReferencingClass):
    indexes = referencing.RefDict(ref_cls=SourceIndex, compcoef=0.909)

    def add_index(self, index):
        self.add_classref('indexes', index)

    def del_index(self, index, schema):
        self.del_classref('indexes', index, schema)
