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


from __future__ import annotations

import textwrap
from typing import *

from ..common import qname as qn
from ..common import quote_ident as qi
from ..common import quote_literal as ql
from ..common import quote_type as qt

from . import base
from . import ddl

FunctionArgType = str | Tuple[str, ...]
FunctionArgTyped = Tuple[Optional[str], FunctionArgType]
FunctionArgDefaulted = Tuple[Optional[str], FunctionArgType, str]
FunctionArg = str | FunctionArgTyped | FunctionArgDefaulted


class Function(base.DBObject):
    def __init__(
        self,
        name: Tuple[str, ...],
        *,
        args: Optional[Sequence[FunctionArg]] = None,
        returns: str | Tuple[str, ...],
        text: str,
        volatility: str = "volatile",
        language: str = "sql",
        has_variadic: Optional[bool] = None,
        strict: bool = False,
        set_returning: bool = False,
    ):
        self.name = name
        self.args = args
        self.returns = returns
        self.text = text
        self.volatility = volatility
        self.language = language
        self.has_variadic = has_variadic
        self.strict = strict
        self.set_returning = set_returning

    def __repr__(self):
        return '<{} {} at 0x{}>'.format(
            self.__class__.__name__, self.name, id(self))


class FunctionExists(base.Condition):
    def __init__(self, name, args=None):
        self.name = name
        self.args = args

    def code(self, block: base.PLBlock) -> str:
        args = f"ARRAY[{','.join(qi(a) for a in self.args)}]"

        return textwrap.dedent(f'''\
            SELECT
                p.proname
            FROM
                pg_catalog.pg_proc p
                INNER JOIN pg_catalog.pg_namespace ns
                    ON (ns.oid = p.pronamespace)
            WHERE
                p.proname = {ql(self.name[1])}
                AND ns.nspname = {ql(self.name[0])}
                AND {args}::text[] = ARRAY(
                    SELECT
                        format_type(t, NULL)::text
                    FROM
                        unnest(p.proargtypes) AS t)
        ''')


class FunctionOperation:
    def format_args(
        self,
        args: Optional[Sequence[FunctionArg]],
        has_variadic: Optional[bool],
        *,
        include_defaults: bool = True,
    ):
        if not args:
            return ''

        args_buf = []
        for argi, arg in enumerate(args, 1):
            vararg = has_variadic and (len(args) == argi)
            arg_expr = 'VARIADIC ' if vararg else ''

            if isinstance(arg, tuple):
                if arg[0] is not None:
                    arg_expr += qn(arg[0])
                if len(arg) > 1:
                    arg_expr += ' ' + qt(arg[1])
                if include_defaults:
                    if len(arg) > 2:
                        arg_def = cast(FunctionArgDefaulted, arg)
                        if arg_def[2] is not None:
                            arg_expr += ' = ' + arg_def[2]

            else:
                arg_expr = arg

            args_buf.append(arg_expr)

        return ', '.join(args_buf)


class CreateFunction(ddl.DDLOperation, FunctionOperation):
    def __init__(
        self, function: Function, *, or_replace: bool = False, **kwargs
    ):
        super().__init__(**kwargs)
        self.function = function
        self.or_replace = or_replace

    def code(self, block: base.PLBlock) -> str:
        args = self.format_args(self.function.args, self.function.has_variadic)

        code = textwrap.dedent('''
            CREATE {replace} FUNCTION {name}({args})
            RETURNS {setof} {returns}
            AS $____funcbody____$
            {text}
            $____funcbody____$
            LANGUAGE {lang} {volatility} {strict};
        ''').format_map({
            'replace': 'OR REPLACE' if self.or_replace else '',
            'name': qn(*self.function.name),
            'args': args,
            'returns': qt(self.function.returns),
            'lang': self.function.language,
            'volatility': self.function.volatility.upper(),
            'text': textwrap.dedent(self.function.text).strip(),
            'strict': 'STRICT' if self.function.strict else '',
            'setof': 'SETOF' if self.function.set_returning else '',
        })
        return code.strip()


class CreateOrReplaceFunction(ddl.DDLOperation, FunctionOperation):
    def __init__(self, function, **kwargs):
        super().__init__(**kwargs)
        self.function = function

    def code(self, block: base.PLBlock) -> str:
        args = self.format_args(self.function.args, self.function.has_variadic)
        ret = self.function.returns
        if isinstance(ret, tuple):
            returns = f'{qi(ret[0])}.{qt(ret[1])}'
        else:
            returns = qt(ret)

        code = textwrap.dedent('''
            CREATE OR REPLACE FUNCTION {name}({args})
            RETURNS {setof} {returns}
            AS $____funcbody____$
            {text}
            $____funcbody____$
            LANGUAGE {lang} {volatility} {strict};
        ''').format_map({
            'name': qn(*self.function.name),
            'args': args,
            'returns': returns,
            'lang': self.function.language,
            'volatility': self.function.volatility.upper(),
            'text': textwrap.dedent(self.function.text).strip(),
            'strict': 'STRICT' if self.function.strict else '',
            'setof': 'SETOF' if self.function.set_returning else '',
        })
        return code.strip()


class DropFunction(ddl.DDLOperation, FunctionOperation):
    def __init__(
        self,
        name: Tuple[str, ...],
        args: Sequence[FunctionArg],
        *,
        if_exists: bool = False,
        has_variadic: bool = False,
        conditions: Optional[List[str | base.Condition]] = None,
        neg_conditions: Optional[List[str | base.Condition]] = None,
    ):
        self.conditional = if_exists
        if conditions:
            c = []
            for cond in conditions:
                if (isinstance(cond, FunctionExists) and
                        cond.name == name and cond.args == args):
                    self.conditional = True
                else:
                    c.append(cond)
            conditions = c
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.name = name
        self.args = args
        self.has_variadic = has_variadic

    def code(self, block: base.PLBlock) -> str:
        ifexists = ' IF EXISTS' if self.conditional else ''
        args = self.format_args(self.args, self.has_variadic,
                                include_defaults=False)
        return f'DROP FUNCTION{ifexists} {qn(*self.name)}({args})'
