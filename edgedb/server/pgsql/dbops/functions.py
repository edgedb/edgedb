##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import textwrap

from .. import common
from . import base
from . import ddl


class Function(base.DBObject):
    def __init__(self, name, *, args=None, returns, text,
                 volatility='volatile', language='sql',
                 variadic_arg=None):
        self.name = name
        self.args = args
        self.returns = returns
        self.text = text
        self.volatility = volatility
        self.language = language
        self.variadic_arg = variadic_arg

    def __repr__(self):
        return '<{} {} at 0x{}>'.format(
            self.__class__.__name__, self.name, id(self))


class FunctionExists(base.Condition):
    def __init__(self, name, args=None):
        self.name = name
        self.args = args

    async def code(self, context):
        code = '''
            SELECT
                p.proname
            FROM
                pg_catalog.pg_proc p
                INNER JOIN pg_catalog.pg_namespace ns
                    ON (ns.oid = p.pronamespace)
            WHERE
                p.proname = $2 AND ns.nspname = $1
                AND ($3::text[] IS NULL
                     OR $3::text[] = ARRAY(SELECT
                                              format_type(t, NULL)::text
                                            FROM
                                              unnest(p.proargtypes) t))
        '''

        return code, self.name + (self.args, )


class CreateFunction(ddl.DDLOperation):
    def __init__(self, function, **kwargs):
        super().__init__(**kwargs)
        self.function = function

    async def code(self, context):
        args = []
        if self.function.args:
            for argi, arg in enumerate(self.function.args, 1):
                vararg = self.function.variadic_arg == argi
                arg_expr = 'VARIADIC ' if vararg else ''

                if isinstance(arg, tuple):
                    if arg[0] is not None:
                        arg_expr += common.qname(arg[0])
                    if len(arg) > 1:
                        arg_expr += ' ' + common.quote_type(arg[1])
                        if vararg:
                            arg_expr += '[]'
                    if len(arg) > 2 and arg[2] is not None:
                        arg_expr += ' = ' + arg[2]

                else:
                    arg_expr = arg

                args.append(arg_expr)

        code = textwrap.dedent('''
            CREATE FUNCTION {name}({args})
            RETURNS {returns}
            AS $____funcbody____$
            {text}
            $____funcbody____$
            LANGUAGE {lang} {volatility};
        ''').format(**{
            'name': common.qname(*self.function.name),
            'args': ', '.join(args),
            'returns': common.quote_type(self.function.returns),
            'lang': self.function.language,
            'volatility': self.function.volatility.upper(),
            'text': textwrap.dedent(self.function.text).strip()
        })
        return code.strip()


class RenameFunction(base.CommandGroup):
    def __init__(
            self, name, args, new_name, *, conditions=None,
            neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        if name[0] != new_name[0]:
            cmd = AlterFunctionSetSchema(name, args, new_name[0])
            self.add_command(cmd)
            name = (new_name[0], name[1])

        if name[1] != new_name[1]:
            cmd = AlterFunctionRenameTo(name, args, new_name[1])
            self.add_command(cmd)


class AlterFunctionReplaceText(ddl.DDLOperation):
    def __init__(
            self, name, args, new_text, *, conditions=None,
            neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name
        self.args = args
        self.new_text = new_text

    async def code(self, context):
        code = '''
            SELECT
                $4::text AS text,
                l.lanname AS lang,
                p.provolatile AS volatility,
                retns.nspname AS retnamens,
                ret.typname AS retname
            FROM
                pg_catalog.pg_proc p
                INNER JOIN pg_catalog.pg_namespace ns
                    ON (ns.oid = p.pronamespace)
                INNER JOIN pg_catalog.pg_language l
                    ON (p.prolang = l.oid)
                INNER JOIN pg_catalog.pg_type ret
                    ON (p.prorettype = ret.oid)
                INNER JOIN pg_catalog.pg_namespace retns
                    ON (retns.oid = ret.typnamespace)
            WHERE
                p.proname = $2 AND ns.nspname = $1
                AND ($3::text[] IS NULL
                     OR $3::text[] = ARRAY(SELECT
                                              format_type(t, NULL)::text
                                            FROM
                                              unnest(p.proargtypes) t))
        '''

        vars = self.name + (self.args, self.new_text)
        new_text, lang, volatility, *returns = \
            await context.db.fetchrow(code, *vars)

        code = '''CREATE OR REPLACE FUNCTION {name} ({args})
                  RETURNS {returns}
                  LANGUAGE {lang}
                  {volatility}
                  AS $____funcbody____$
                      {text}
                  $____funcbody____$;
               '''.format(
            name=common.qname(*self.name),
            args=', '.join(common.quote_ident(a) for a in self.args),
            text=new_text, lang=lang, returns=common.qname(*returns),
            volatility={b'i': 'IMMUTABLE',
                        b's': 'STABLE',
                        b'v': 'VOLATILE'}[volatility])

        return code, ()


class AlterFunctionSetSchema(ddl.DDLOperation):
    def __init__(
            self, name, args, new_schema, *, conditions=None,
            neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name
        self.args = args
        self.new_schema = new_schema

    async def code(self, context):
        code = 'ALTER FUNCTION {}({}) SET SCHEMA {}'.format(
            common.qname(*self.name),
            ', '.join(common.quote_ident(a) for a in self.args),
            common.quote_ident(self.new_schema))
        return code


class AlterFunctionRenameTo(ddl.DDLOperation):
    def __init__(
            self, name, args, new_name, *, conditions=None,
            neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name
        self.args = args
        self.new_name = new_name

    async def code(self, context):
        code = 'ALTER FUNCTION {}({}) RENAME TO {}'.format(
            common.qname(*self.name),
            ', '.join(common.quote_ident(a) for a in self.args),
            common.quote_ident(self.new_name))
        return code


class DropFunction(ddl.DDLOperation):
    def __init__(
            self, name, args, *, conditions=None, neg_conditions=None,
            priority=0):
        self.conditional = False
        if conditions:
            c = []
            for cond in conditions:
                if (isinstance(cond, FunctionExists) and
                        cond.name == name and cond.args == args):
                    self.conditional = True
                else:
                    c.append(cond)
            conditions = c
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name
        self.args = args

    async def code(self, context):
        code = 'DROP FUNCTION{} {}({})'.format(
            ' IF EXISTS' if self.conditional else '',
            common.qname(*self.name),
            ', '.join(common.quote_ident(a) for a in self.args))
        return code
