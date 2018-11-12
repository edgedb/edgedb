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

    operator_kind = so.Field(ft.OperatorKind, coerce=True, compcoef=0.4)

    language = so.Field(qlast.Language, default=None, compcoef=0.4,
                        coerce=True)
    from_operator = so.Field(str, default=None, compcoef=0.4,
                             introspectable=False)
    commutator = so.Field(so.Object, default=None, compcoef=0.99)


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
    def _get_operator_fullname(
            cls, schema, name: str, kind: ft.OperatorKind,
            params: s_func.FuncParameterList) -> sn.Name:
        quals = cls._get_operator_name_quals(schema, name, kind, params)
        return sn.Name(
            module=name.module,
            name=named.NamedObject.get_specialized_name(name, *quals))

    @classmethod
    def _classname_from_ast(cls, schema, astnode: qlast.OperatorCommand,
                            context) -> sn.Name:
        name = super()._classname_from_ast(schema, astnode, context)

        params = s_func.FuncParameterList.from_ast(
            astnode, context.modaliases, schema)

        return cls._get_operator_fullname(schema, name, astnode.kind, params)

    def _qualify_operator_refs(
            self, kind: ft.OperatorKind, params: s_func.FuncParameterList,
            context, schema):

        self_shortname = named.NamedObject.get_shortname(self.classname)
        commutator = self.get_attribute_value('commutator')
        if commutator is not None:
            if commutator.classname == self_shortname:
                commutator.classname = self.classname
            else:
                opers = schema.get_operators(commutator.classname)

                for oper in opers:
                    if (oper.operator_kind == kind and
                            len(oper.params) == len(params) and
                            all(p1.type == p2.type and
                                p1.typemod == p2.typemod
                                for p1, p2 in zip(oper.params, params))):
                        commutator.classname = oper.name
                        break
                else:
                    raise ql_errors.EdgeQLError(
                        f'operator {commutator.classname} {params.as_str} '
                        f'does not exist',
                        context=self.source_context,
                    )


class CreateOperator(s_func.CreateCallableObject, OperatorCommand):
    astnode = qlast.CreateOperator

    def _add_to_schema(self, schema, context):
        params: s_func.FuncParameterList = self.scls.params
        name = self.scls.name
        return_type = self.scls.return_type
        return_typemod = self.scls.return_typemod

        get_signature = lambda: f'{self.classname}{params.as_str()}'

        oper = schema.get(name, None)
        if oper:
            raise ql_errors.EdgeQLError(
                f'cannot create {get_signature()} operator: '
                f'an operator with the same signature '
                f'is already defined',
                context=self.source_context)

        shortname = named.NamedObject.get_shortname(name)
        for oper in schema.get_operators(shortname, ()):
            if oper.return_typemod != return_typemod:
                raise ql_errors.EdgeQLError(
                    f'cannot create {get_signature()} -> '
                    f'{return_typemod.to_edgeql()} {return_type.name} '
                    f'operator: overloading another operator with different '
                    f'return type {oper.return_typemod.to_edgeql()} '
                    f'{oper.return_type.name}',
                    context=self.source_context)

        return super()._add_to_schema(schema, context)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        modaliases = context.modaliases

        params = s_func.FuncParameterList.from_ast(astnode, modaliases, schema,
                                                   func_fqname=cmd.classname)

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

        cmd._qualify_operator_refs(astnode.kind, params, context, schema)
        return cmd


class RenameOperator(named.RenameNamedObject, OperatorCommand):
    pass


class AlterOperator(named.AlterNamedObject, OperatorCommand):
    astnode = qlast.AlterOperator

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        modaliases = context.modaliases
        params = s_func.FuncParameterList.from_ast(astnode, modaliases, schema)

        cmd._qualify_operator_refs(astnode.kind, params, context, schema)
        return cmd


class DeleteOperator(s_func.DeleteCallableObject, OperatorCommand):
    astnode = qlast.DropOperator
