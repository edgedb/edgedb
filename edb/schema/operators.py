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

from edb import errors
from edb.common import typed

from edb.edgeql import ast as qlast
from edb.edgeql import functypes as ft

from . import abc as s_abc
from . import delta as sd
from . import functions as s_func
from . import name as sn
from . import objects as so
from . import utils


class Operator(s_func.CallableObject, s_abc.Operator):

    operator_kind = so.SchemaField(
        ft.OperatorKind, coerce=True, compcoef=0.4)

    language = so.SchemaField(
        qlast.Language, default=None, compcoef=0.4, coerce=True)

    from_operator = so.SchemaField(
        typed.StrList, coerce=True,
        default=None, compcoef=0.4, introspectable=False)

    from_function = so.SchemaField(
        str, default=None, compcoef=0.4, introspectable=False)

    from_expr = so.SchemaField(
        bool, default=False, compcoef=0.4, introspectable=False)

    force_return_cast = so.SchemaField(
        bool, default=False, compcoef=0.9, introspectable=False)

    code = so.SchemaField(
        str, default=None, compcoef=0.4, introspectable=False)

    commutator = so.SchemaField(
        so.Object, default=None, compcoef=0.99)

    recursive = so.SchemaField(
        bool, default=False, compcoef=0.4, introspectable=False)

    def get_display_signature(self, schema):
        params = [
            p.get_type(schema).get_displayname(schema)
            for p in self.get_params(schema).objects(schema)
        ]
        name = self.get_shortname(schema).name
        kind = self.get_operator_kind(schema)
        if kind is ft.OperatorKind.INFIX:
            return f'{params[0]} {name} {params[1]}'
        elif kind is ft.OperatorKind.POSTFIX:
            return f'{params[0]} {name}'
        elif kind is ft.OperatorKind.PREFIX:
            return f'{name} {params[1]}'
        else:
            raise ValueError('unexpected operator kind')


class OperatorCommandContext(s_func.CallableCommandContext):
    pass


class OperatorCommand(s_func.CallableCommand,
                      schema_metaclass=Operator,
                      context_class=OperatorCommandContext):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        if not context.stdmode and not context.testmode:
            raise errors.UnsupportedFeatureError(
                'user-defined operators are not supported',
                context=astnode.context
            )

        return super()._cmd_tree_from_ast(schema, astnode, context)

    @classmethod
    def _classname_from_ast(cls, schema, astnode: qlast.OperatorCommand,
                            context) -> sn.Name:
        name = super()._classname_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        return cls.get_schema_metaclass().get_fqname(
            schema, name, params, astnode.kind)

    def _qualify_operator_refs(
            self, schema, kind: ft.OperatorKind,
            params: typing.List[s_func.ParameterDesc], context):

        self_shortname = sn.shortname_from_fullname(self.classname)
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
                raise errors.QueryError(
                    f'operator {commutator.classname} {params.as_str(schema)} '
                    f'does not exist',
                    context=self.source_context,
                )


class CreateOperator(s_func.CreateCallableObject, OperatorCommand):
    astnode = qlast.CreateOperator

    def _create_begin(self, schema, context):

        fullname = self.classname
        shortname = sn.shortname_from_fullname(fullname)
        cp = self._get_param_desc_from_delta(schema, self)
        signature = f'{shortname}({", ".join(p.as_str(schema) for p in cp)})'

        func = schema.get(fullname, None)
        if func:
            raise errors.InvalidOperatorDefinitionError(
                f'cannot create the `{signature}` operator: '
                f'an operator with the same signature '
                f'is already defined',
                context=self.source_context)

        schema = super()._create_begin(schema, context)

        params: s_func.FuncParameterList = self.scls.get_params(schema)
        fullname = self.scls.get_name(schema)
        shortname = sn.shortname_from_fullname(fullname)
        return_type = self.scls.get_return_type(schema)
        return_typemod = self.scls.get_return_typemod(schema)
        recursive = self.scls.get_recursive(schema)

        get_signature = lambda: f'{shortname}{params.as_str(schema)}'

        # an operator must have operands
        if len(params) == 0:
            raise errors.InvalidOperatorDefinitionError(
                f'cannot create the `{signature}` operator: '
                f'an operator must have operands',
                context=self.source_context)

        # We'll need to make sure that there's no mix of recursive and
        # non-recursive operators being overloaded.
        all_arrays = all_tuples = True
        for param in params.objects(schema):
            ptype = param.get_type(schema)
            all_arrays = all_arrays and ptype.is_array()
            all_tuples = all_tuples and ptype.is_tuple()

        # It's illegal to declare an operator as recursive unless all
        # of its operands are the same basic type of collection.
        if recursive and not (all_arrays or all_tuples):
            raise errors.InvalidOperatorDefinitionError(
                f'cannot create the `{signature}` operator: '
                f'operands of a recursive operator must either be '
                f'all arrays or all tuples',
                context=self.source_context)

        for oper in schema.get_operators(shortname, ()):
            oper_return_typemod = oper.get_return_typemod(schema)
            if oper_return_typemod != return_typemod:
                raise errors.DuplicateOperatorDefinitionError(
                    f'cannot create the `{get_signature()} -> '
                    f'{return_typemod.to_edgeql()} {return_type.name}` '
                    f'operator: overloading another operator with different '
                    f'return type {oper_return_typemod.to_edgeql()} '
                    f'{oper.get_return_type(schema).name}',
                    context=self.source_context)

            # Check if there is a recursive/non-recursive operator
            # overloading.
            oper_recursive = oper.get_recursive(schema)
            if recursive != oper_recursive:
                oper_signature = oper.get_display_signature(schema)
                oper_all_arrays = oper_all_tuples = True
                for param in oper.get_params(schema).objects(schema):
                    ptype = param.get_type(schema)
                    oper_all_arrays = oper_all_arrays and ptype.is_array()
                    oper_all_tuples = oper_all_tuples and ptype.is_tuple()

                if (all_arrays == oper_all_arrays and
                        all_tuples == oper_all_tuples):
                    new_rec = 'recursive' if recursive else 'non-recursive'
                    oper_rec = \
                        'recursive' if oper_recursive else 'non-recursive'

                    raise errors.InvalidOperatorDefinitionError(
                        f'cannot create the {new_rec} `{signature}` operator: '
                        f'overloading a {oper_rec} operator '
                        f'`{oper_signature}` with a {new_rec} one '
                        f'is not allowed',
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
            if astnode.code.from_operator is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_operator',
                    new_value=astnode.code.from_operator
                ))
            if astnode.code.from_function is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_function',
                    new_value=astnode.code.from_function
                ))
            if astnode.code.code is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='code',
                    new_value=astnode.code.code
                ))
            if astnode.code.from_expr is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_expr',
                    new_value=astnode.code.from_expr
                ))

        cmd._qualify_operator_refs(schema, astnode.kind, params, context)
        return cmd


class RenameOperator(sd.RenameObject, OperatorCommand):
    pass


class AlterOperator(sd.AlterObject, OperatorCommand):
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
