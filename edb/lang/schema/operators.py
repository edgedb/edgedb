#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


import typing

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors as ql_errors
from edb.lang.edgeql import functypes as ft

from . import delta as sd
from . import functions as s_func
from . import name as sn
from . import named
from . import objects as so
from . import utils


class Operator(s_func.CallableObject):
    _type = 'operator'

    operator_kind = so.SchemaField(
        ft.OperatorKind, coerce=True, compcoef=0.4)

    language = so.SchemaField(
        qlast.Language, default=None, compcoef=0.4, coerce=True)

    from_operator = so.SchemaField(
        str, default=None, compcoef=0.4, introspectable=False)

    commutator = so.SchemaField(
        so.Object, default=None, compcoef=0.99)


class OperatorCommandContext(sd.ObjectCommandContext):
    pass


class OperatorCommand(s_func.CallableCommand,
                      schema_metaclass=Operator,
                      context_class=OperatorCommandContext):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        if not context.stdmode and not context.testmode:
            raise ql_errors.EdgeQLError(
                'user-defined operators are not yet supported',
                context=astnode.context
            )

        return super()._cmd_tree_from_ast(schema, astnode, context)

    @classmethod
    def _get_operator_name_quals(
            cls, schema, name: str, kind: ft.OperatorKind,
            params: s_func.FuncParameterList) -> typing.List[str]:
        quals = super()._get_function_name_quals(schema, name, params)
        quals.append(kind)
        return quals

    @classmethod
    def _classname_from_ast(cls, schema, astnode: qlast.OperatorCommand,
                            context) -> sn.Name:
        name = super()._classname_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        quals = cls._get_operator_name_quals(
            schema, name, astnode.kind, params)

        return sn.Name(
            module=name.module,
            name=Operator.get_specialized_name(name, *quals))

    def _qualify_operator_refs(
            self, schema, kind: ft.OperatorKind,
            params: typing.List[s_func.ParameterDesc], context):

        self_shortname = named.NamedObject.shortname_from_fullname(
            self.classname)
        commutator = self.get_attribute_value('commutator')
        if commutator is None:
            return

        if commutator.classname == self_shortname:
            commutator.classname = self.classname
        else:
            opers = schema.get_operators(commutator.classname)

            for oper in opers:
                oper_params = oper.get_params(schema)
                if (oper.get_operator_kind(schema) == kind and
                        len(oper_params) == len(params) and
                        all(p1.get_type(schema) == p2.get_type(schema) and
                            p1.get_typemod(schema) == p2.get_typemod(schema)
                            for p1, p2 in zip(oper_params, params))):
                    commutator.classname = oper.name
                    break
            else:
                raise ql_errors.EdgeQLError(
                    f'operator {commutator.classname} {params.as_str(schema)} '
                    f'does not exist',
                    context=self.source_context,
                )


class CreateOperator(s_func.CreateCallableObject, OperatorCommand):
    astnode = qlast.CreateOperator

    def _create_begin(self, schema, context):

        fullname = self.classname
        shortname = Operator.shortname_from_fullname(fullname)
        cp = self._get_param_desc_from_delta(schema, self)
        signature = f'{shortname}({", ".join(p.as_str(schema) for p in cp)})'

        func = schema.get(fullname, None)
        if func:
            raise ql_errors.EdgeQLError(
                f'cannot create the `{signature}` operator: '
                f'an operator with the same signature '
                f'is already defined',
                context=self.source_context)

        schema = super()._create_begin(schema, context)

        params: s_func.FuncParameterList = self.scls.get_params(schema)
        fullname = self.scls.get_name(schema)
        shortname = Operator.shortname_from_fullname(fullname)
        return_type = self.scls.get_return_type(schema)
        return_typemod = self.scls.get_return_typemod(schema)

        get_signature = lambda: f'{shortname}{params.as_str(schema)}'

        for oper in schema.get_operators(shortname, ()):
            oper_return_typemod = oper.get_return_typemod(schema)
            if oper_return_typemod != return_typemod:
                raise ql_errors.EdgeQLError(
                    f'cannot create the `{get_signature()} -> '
                    f'{return_typemod.to_edgeql()} {return_type.name}` '
                    f'operator: overloading another operator with different '
                    f'return type {oper_return_typemod.to_edgeql()} '
                    f'{oper.get_return_type(schema).name}',
                    context=self.source_context)

        return schema

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        modaliases = context.modaliases

        params = cls._get_param_desc_from_ast(
            schema, modaliases, astnode)

        cmd.add(sd.AlterObjectProperty(
            property='operator_kind',
            new_value=astnode.kind,
        ))

        cmd.add(sd.AlterObjectProperty(
            property='return_type',
            new_value=utils.ast_to_typeref(
                astnode.returning, modaliases=modaliases, schema=schema)
        ))

        cmd.add(sd.AlterObjectProperty(
            property='return_typemod',
            new_value=astnode.returning_typemod
        ))

        if astnode.code is not None:
            cmd.add(sd.AlterObjectProperty(
                property='language',
                new_value=astnode.code.language
            ))
            if astnode.code.from_name is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_operator',
                    new_value=astnode.code.from_name
                ))

        cmd._qualify_operator_refs(schema, astnode.kind, params, context)
        return cmd


class RenameOperator(named.RenameNamedObject, OperatorCommand):
    pass


class AlterOperator(named.AlterNamedObject, OperatorCommand):
    astnode = qlast.AlterOperator

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        cmd._qualify_operator_refs(schema, astnode.kind, params, context)
        return cmd


class DeleteOperator(s_func.DeleteCallableObject, OperatorCommand):
    astnode = qlast.DropOperator
