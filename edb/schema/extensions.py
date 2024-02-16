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

import contextlib
import uuid

from edb import errors

from edb.common import verutils

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser

from edb.common import checked

from . import annos as s_anno
from . import casts as s_casts
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

    # Note: !!!!!!
    # ExtensionPackage, like all GlobalObjects, needs to store its
    # data in globally stored JSON instead of via reflection schema.
    # When you add a field to ExtensionPackage, you must also update
    # CreateExtensionPackage in pgsql/delta.py and
    # _generate_extension_views in metaschema to store and retrieve
    # the data from json.

    version = so.SchemaField(
        verutils.Version,
        compcoef=0.9,
    )

    script = so.SchemaField(
        str,
        compcoef=0.9,
    )

    sql_extensions = so.SchemaField(
        checked.FrozenCheckedSet[str],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.9,
    )

    ext_module = so.SchemaField(
        str, default=None, compcoef=0.9)

    # It uses str instead of direct references so we can stick
    # versions in there eventually
    dependencies = so.SchemaField(
        checked.FrozenCheckedSet[str],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
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
        compcoef=0.0,
    )

    dependencies = so.SchemaField(
        so.ObjectList['Extension'],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.9,
    )

    @classmethod
    def create_in_schema(
        cls: Type[Extension],
        schema: s_schema.Schema_T,
        stable_ids: bool = False,
        *,
        id: Optional[uuid.UUID] = None,
        **data: Any,
    ) -> Tuple[s_schema.Schema_T, Extension]:
        name = data['name']
        pkg = data['package']

        if existing_ext := schema.get_global(Extension, name, default=None):
            vn = existing_ext.get_verbosename(schema)
            existing_pkg = existing_ext.get_package(schema)
            raise errors.SchemaError(
                f'cannot install {vn} version {pkg.get_version(schema)}: '
                f'version {existing_pkg.get_version(schema)} is already '
                f'installed'
            )

        return super().create_in_schema(schema, stable_ids, id=id, **data)


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


def get_package(
    name: sn.Name, version: Optional[verutils.Version], schema: s_schema.Schema
) -> ExtensionPackage:
    filters = [
        lambda schema, pkg: (
            pkg.get_shortname(schema) == name
        )
    ]
    if version is not None:
        filters.append(
            lambda schema, pkg: (
                pkg.get_version(schema) >= version
                and pkg.get_version(schema).major == version.major
            )
        )

    pkgs = list(schema.get_objects(
        type=ExtensionPackage,
        extra_filters=filters,
    ))

    if not pkgs:
        dname = str(name)
        if version is None:
            raise errors.SchemaError(
                f'cannot create extension {dname!r}:'
                f' extension package {dname!r} does'
                f' not exist'
            )
        else:
            raise errors.SchemaError(
                f'cannot create extension {dname!r}:'
                f' extension package {dname!r} version'
                f' {str(version)!r} does not exist'
            )

    pkgs.sort(key=lambda pkg: pkg.get_version(schema), reverse=True)

    return pkgs[0]


# XXX: Trying to CREATE/DROP these from within a transaction managed
# to get me stuck getting "Cannot serialize global DDL" errors.
#
# I'm haven't fully investigated whether it is actually sensible to do
# this kind of global command in a transaction, but we currently allow
# it and some tests do it.
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
                commands=cast(
                    List[qlast.DDLOperation],
                    qlparser.parse_block(op.new_value)),
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


@contextlib.contextmanager
def _extension_mode(context: sd.CommandContext) -> Iterator[None]:
    # TODO: We'll want to be a bit more discriminating once we support
    # user extensions, and not set stable_ids then?
    stable_ids = context.stable_ids
    testmode = context.testmode
    declarative = context.declarative
    context.stable_ids = True
    context.testmode = True
    context.declarative = False
    try:
        yield
    finally:
        context.stable_ids = stable_ids
        context.testmode = testmode
        context.declarative = declarative


class CreateExtension(
    ExtensionCommand,
    sd.CreateObject[Extension],
):
    astnode = qlast.CreateExtension

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        with _extension_mode(context):
            return super().apply(schema, context)

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

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)

        if not context.canonical:
            package = self.scls.get_package(schema)
            script = package.get_script(schema)
            if script:
                block, _ = qlparser.parse_extension_package_body_block(script)
                for subastnode in block.commands:
                    subcmd = sd.compile_ddl(
                        schema, subastnode, context=context)
                    if subcmd is not None:
                        self.add(subcmd)

        return schema

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        pkg: ExtensionPackage

        if pkg_attr := self.get_attribute_value('package'):
            pkg = pkg_attr.resolve(schema)
        else:
            # If we're restoring a dump ignore the extension package version
            # as the current EdgeDB might have a different version available
            # and we don't have a way to select specific versions yet.
            #
            # Use `compat_ver` as a way to detect that we're working with a
            # dump rather than some other operation.
            if context.compat_ver is not None:
                version = None
            else:
                version = self.get_attribute_value('version')

            pkg = get_package(self.classname, version, schema)

        self.discard_attribute('version')

        self.set_attribute_value('package', pkg)

        deps = []
        for dep_name in pkg.get_dependencies(schema):
            if '==' not in dep_name:
                raise errors.SchemaError(
                    f'built-in extension {self.classname} missing '
                    f'version for {dep_name}')
            dep_name, dep_version_s = dep_name.split('==')
            dep = schema.get_global(Extension, dep_name, default=None)
            if not dep:
                raise errors.SchemaError(
                    f'cannot create extension {self.get_displayname()!r}:'
                    f' it depends on extension {dep_name} which has not been'
                    f' created'
                )
            dep_version = verutils.parse_version(dep_version_s)
            real_version = dep.get_package(schema).get_version(schema)
            if dep_version != real_version:
                raise errors.SchemaError(
                    f'cannot create extension {self.get_displayname()!r} :'
                    f'it depends on extension {dep_name}, but the wrong '
                    f'version is installed: {real_version} is present but '
                    f'{dep_version} is required'
                )

            deps.append(dep)

        self.set_attribute_value('dependencies', deps)

        return schema

    # XXX: I think this is wrong, but it might not matter ever.
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
        # When performing dumps we don't want to include the extension version
        # as we're not guaranteed that the same version will be avaialble when
        # restoring the dump. We also have no mechanism of installing a specific
        # extension version, yet.
        if context.include_ext_version:
            node.version = qlast.StringConstant(
                value=str(pkg.get_version(schema))
            )
        return node


class DeleteExtension(
    ExtensionCommand,
    sd.DeleteObject[Extension],
):

    astnode = qlast.DropExtension

    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        module = self.scls.get_package(schema).get_ext_module(schema)
        schema = super()._delete_begin(schema, context)

        if context.canonical or not module:
            return schema

        # If the extension included a module, delete everything in it.
        from . import ddl as s_ddl

        module_name = sn.UnqualName(module)

        def _name_in_mod(name: sn.Name) -> bool:
            return (
                (isinstance(name, sn.QualName) and name.module == module)
                or name == module_name
            )

        # Clean up the casts separately for annoying reasons
        for obj in schema.get_objects(
            included_modules=(sn.UnqualName('__derived__'),),
            type=s_casts.Cast,
        ):
            if (
                _name_in_mod(obj.get_from_type(schema).get_name(schema))
                or _name_in_mod(obj.get_to_type(schema).get_name(schema))
            ):
                drop = obj.init_delta_command(
                    schema,
                    sd.DeleteObject,
                )
                self.add(drop)

        def filt(schema: s_schema.Schema, obj: so.Object) -> bool:
            return not _name_in_mod(obj.get_name(schema)) or obj == self.scls

        # We handle deleting the module contents in a heavy-handed way:
        # do a schema diff.
        delta = s_ddl.delta_schemas(
            schema, schema,
            included_modules=[
                sn.UnqualName(module),
            ],
            schema_b_filters=[filt],
            include_extensions=True,
            linearize_delta=True,
        )
        # The output of delta_schemas is really just intended to be
        # dumped as an AST. So, sigh, just do that, and then read it
        # back.
        #
        # This is horrific, but it does actually work and is built
        # around codepaths that are heavily tested.
        from . import ddl
        for subast in ddl.ddlast_from_delta(None, schema, delta):
            self.add(sd.compile_ddl(schema, subast, context=context))

        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        with _extension_mode(context):
            return super().apply(schema, context)
