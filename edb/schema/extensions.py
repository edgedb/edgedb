#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations
from typing import *

from edb import errors

from edb.common import verutils

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser

from . import annos as s_anno
from . import delta as sd
from . import name as sn
from . import objects as so
from . import schema as s_schema


class ExtensionPackage(
    so.GlobalObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.EXTENSION_PACKAGE,
    data_safe=False,
):

    version = so.SchemaField(
        verutils.Version,
        compcoef=0.9,
    )

    script = so.SchemaField(
        str,
        compcoef=0.9,
    )

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'extension package'

    @classmethod
    def get_shortname_static(cls, name: sn.Name) -> sn.UnqualName:
        return sn.UnqualName(sn.shortname_from_fullname(name).name)

    @classmethod
    def get_displayname_static(cls, name: sn.Name) -> str:
        shortname = cls.get_shortname_static(name)
        return shortname.name


class Extension(
    so.Object,
    qlkind=qltypes.SchemaObjectClass.EXTENSION,
    data_safe=False,
):

    package = so.SchemaField(
        ExtensionPackage,
    )


class ExtensionPackageCommandContext(
    sd.ObjectCommandContext[ExtensionPackage],
    s_anno.AnnotationSubjectCommandContext,
):
    pass


class ExtensionPackageCommand(
    sd.GlobalObjectCommand[ExtensionPackage],
    s_anno.AnnotationSubjectCommand[ExtensionPackage],
    context_class=ExtensionPackageCommandContext,
):

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext
    ) -> sn.UnqualName:
        assert isinstance(astnode, qlast.ExtensionPackageCommand)
        parsed_version = verutils.parse_version(astnode.version.value)
        quals = ['pkg', str(parsed_version)]
        pnn = sn.get_specialized_name(sn.UnqualName(astnode.name.name), *quals)
        return sn.UnqualName(pnn)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        if not context.stdmode and not context.testmode:
            raise errors.UnsupportedFeatureError(
                'user-defined extension packages are not supported yet',
                context=astnode.context
            )

        return super()._cmd_tree_from_ast(schema, astnode, context)


class CreateExtensionPackage(
    ExtensionPackageCommand,
    sd.CreateObject[ExtensionPackage],
):
    astnode = qlast.CreateExtensionPackage

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> CreateExtensionPackage:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, CreateExtensionPackage)
        assert isinstance(astnode, qlast.CreateExtensionPackage)
        assert astnode.body.text is not None

        parsed_version = verutils.parse_version(astnode.version.value)
        cmd.set_attribute_value('version', parsed_version)
        cmd.set_attribute_value('script', astnode.body.text)
        cmd.set_attribute_value('builtin', context.stdmode)

        if not cmd.has_attribute_value('internal'):
            cmd.set_attribute_value('internal', False)

        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        assert isinstance(node, qlast.CreateExtensionPackage)
        if op.property == 'script':
            node.body = qlast.NestedQLBlock(
                text=op.new_value,
                commands=qlparser.parse_block(op.new_value),
            )
        elif op.property == 'version':
            node.version = qlast.StringConstant(
                value=str(op.new_value),
            )
        else:
            super()._apply_field_ast(schema, context, node, op)


class DeleteExtensionPackage(
    ExtensionPackageCommand,
    sd.DeleteObject[ExtensionPackage],
):
    astnode = qlast.DropExtensionPackage


class ExtensionCommandContext(
    sd.ObjectCommandContext[Extension],
):
    pass


class ExtensionCommand(
    sd.ObjectCommand[Extension],
    context_class=ExtensionCommandContext,
):

    pass


class CreateExtension(
    ExtensionCommand,
    sd.CreateObject[Extension],
):
    astnode = qlast.CreateExtension

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext
    ) -> CreateExtension:
        assert isinstance(astnode, qlast.CreateExtension)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, CreateExtension)

        if astnode.version is not None:
            parsed_version = verutils.parse_version(astnode.version.value)
            cmd.set_attribute_value('version', parsed_version)

        return cmd

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        filters = [
            lambda schema, pkg: (
                pkg.get_shortname(schema) == self.classname
            )
        ]
        version = self.get_attribute_value('version')
        if version is not None:
            filters.append(
                lambda schema, pkg: pkg.get_version(schema) == version,
            )
            self.discard_attribute('version')

        pkgs = list(schema.get_objects(
            type=ExtensionPackage,
            extra_filters=filters,
        ))

        if not pkgs:
            if version is None:
                raise errors.SchemaError(
                    f'cannot create extension {self.get_displayname()!r}:'
                    f' extension package {self.get_displayname()!r} does'
                    f' not exist'
                )
            else:
                raise errors.SchemaError(
                    f'cannot create extension {self.get_displayname()!r}:'
                    f' extension package {self.get_displayname()!r} version'
                    f' {str(version)!r} does not exist'
                )

        pkgs.sort(key=lambda pkg: pkg.get_version(schema), reverse=True)
        self.set_attribute_value('package', pkgs[0])
        return schema

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        node = super()._get_ast(schema, context, parent_node=parent_node)
        assert isinstance(node, qlast.CreateExtension)
        pkg = self.get_resolved_attribute_value(
            'package', schema=schema, context=context)
        node.version = qlast.StringConstant(
            value=str(pkg.get_version(schema))
        )
        return node


class DeleteExtension(
    ExtensionCommand,
    sd.DeleteObject[Extension],
):

    astnode = qlast.DropExtension
