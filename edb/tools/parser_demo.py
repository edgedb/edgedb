#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


from edb.edgeql import ast as qlast
from edb.edgeql import tokenizer
from edb.edgeql import parser as qlparser
from edb.edgeql.parser.grammar import tokens as qltokens

import edb._edgeql_parser as rust_parser

from edb.tools.edb import edbcommands


@edbcommands.command("parser-demo")
def main():
    qlparser.preload_spec()

    for q in QUERIES[-8:]:
        sdl = q.startswith('sdl')
        if sdl:
            q = q[3:]

        try:
            source = tokenizer.NormalizedSource.from_string(q)
            # source = tokenizer.Source.from_string(q)
        except Exception as e:
            print('Error during tokenization:')
            print(e)
            continue

        start_t = qltokens.T_STARTSDLDOCUMENT if sdl else qltokens.T_STARTBLOCK
        start_t_name = start_t.__name__[2:]
        tokens = source.tokens()
        result, productions = rust_parser.parse(start_t_name, tokens)

        print('-' * 30)
        print()

        for index, error in enumerate(result.errors):
            message, span, hint, details = error
            (start, end) = tokenizer.inflate_span(source.text(), span)

            print(f'Error [{index + 1}/{len(result.errors)}]:')
            print(
                '\n'.join(
                    source.text().splitlines()[(start.line - 1) : end.line]
                )
            )
            print(
                ' ' * (start.column - 1)
                + '^' * (max(1, end.column - start.column))
                + ' '
                + message
            )
            if details:
                print(f'  Details: {details}')
            if hint:
                print(f'  Hint: {hint}')
            print()

        if result.out:
            try:
                ast = qlparser._cst_to_ast(
                    result.out, productions, source=source, filename=''
                ).val
            except Exception as e:
                print(e)
                ast = None
            if ast:
                print('Recovered AST:')
                if isinstance(ast, list):
                    for x in ast:
                        assert isinstance(x, qlast.Base)
                        x.dump_edgeql()
                        x.dump()
                        print(x.span.start, x.span.end)
                elif isinstance(ast, qlast.Base):
                    ast.dump_edgeql()
                    ast.dump()
                    print(ast.span.start, ast.span.end)
                else:
                    print(ast)


QUERIES = [
    '''
        select 1
    ''',
    '''
        select User { name, email } filter .name = 'Sully'
    ''',
    '''
        SELECT {354.32,
            35400000000000.32,
            35400000000000000000.32,
            3.5432e20,
            3.5432e+20,
            3.5432e-20,
            3.543_2e-20,
            354.32e-20,
            2_354.32e-20,
            0e-999
        }
        ''',
    '''
        with module cards
        for g in (group Card by .element) union (for gi in 0 union (
            element := g.key.element,
            cst := sum(g.elements.cost + gi),
        ))
        ''',
    '''
    select '10 seconds'
    ''',
    '''SELECT (User.id, User { name := ''',
    '''SELECT (false, }]})''',
    '''
    SELECT User { name, last_name }
    WITH u := User SELECT u;
    ''',
    '''
    SELECT (false, true false])
    ''',
    '''
    for c Card union c.hello
    ''',
    '''
    SELECT User id, name }
    ''',
    '''
    CREATE TYPE cfg::TestSessionConfig EXTENDING cfg::ConfigObject {
        CREATE REQUIRED PROPERTY name -> std::str {
            CREATE CONSTRAINT std::exclusive;
        }
    };
    ''',
    '''
    CREATE FUNCTION
    std::_gen_series(
        `start`: std::int64,
        stop: std::int64
    ) -> SET OF std::int64
    {
        SET volatility := 'Immutable';
        USING SQL FUNCTION 'generate_series';
    };
    ''',
    '''
    select b"04e3b";
    ''',
    '''
    select User { intersect };
    ''',
    '''
    create module __std__;
    ''',
    '''
    create type Hello {
        create property intersect -> str;
        create property `__std__` -> str;
    };
    ''',
    '''
    SELECT
    count(
        schema::Module
        FILTER NOT .builtin AND NOT .name = "default"
    ) + count(
        schema::Object
        FILTER .name LIKE "default::%"
    ) > 0
    ''',
    '''sdl
    module test {
        function len1(a: str b: str) ->  std::str {
            using SQL function 'length1'
        }
    ''',
    '''
    SELECT len('');
    ''',
    '''
    SELECT __std__::len({'hello', 'world'});
    ''',
    '''sdl
    module test {
        alias FooBaz := [1 2];
    };
    ''',
    '''
    SEL ECT 1
    ''',
    '''
    SELECT (
        foo: 1,
        bar := 3
    );
    ''',
    '''
    SELECT (
        foo: (
            bar: 42
        )
    );
    ''',
    '''
    SELECT count(FOR X IN {Foo} UNION X);
    ''',
    '''
    SELECT some_agg(User.name) OVER (ORDER BY User.age ASC);
    SELECT some_agg(User.name) OVER (
        PARTITION BY strlen(User.name)
        ORDER BY User.age ASC);
    SELECT some_agg(User.name) OVER (
        PARTITION BY User.email, User.age
        ORDER BY User.age ASC);
    SELECT some_agg(User.name) OVER (
        PARTITION BY User.email, User.age
        ORDER BY User.age ASC THEN User.name ASC);
    ''',
    '''
    SELECT Issue{
            name,
            related_to *-1,
        };
    ''',
    '''
    SELECT __type__;
    ''',
    '''
    SELECT Issue{
        name,
        related_to *,
    };
    ''',
    '''
    SELECT Foo {(bar)};
    ''',
    '''
    SELECT Foo.__source__;
    ''',
    '''
    SELECT Foo.bar@__type__;
    ''',
    '''
    SELECT Foo {
        __type__.name
    };
    ''',
    '''
    SELECT INTROSPECT tuple<int64>;
    ''',
    '''
    CREATE FUNCTION std::strlen(string: std::str = '1', abc: std::str)
            -> std::int64 {};
    ''',
    '''
    SELECT Obj.n + random()
    ''',
    '''
    CREATE MIGRATION { ;;; CREATE TYPE Foo ;;; CREATE TYPE Bar ;;; };
    ''',
    '''
    SELECT (User IS (Named, Text));
    ''',
    '''sdl
    module test {
        scalar type foobar {
            index prop on (__source__);
        };
    };
    ''',
    '''
    INSERT Foo FILTER Foo.bar = 42;
    ''',
    '''sdl
    module test {
        function some_func($`(`: str = ) ) -> std::str {
            using edgeql function 'some_other_func';
        }
    };
    ''',
    '''
    SELECT (a := 1, foo);
    ''',
    '''
    CREATE MODULE `__std__`;
    ''',
    '''
    SELECT ((((count(foo 1)))));
    ''',
    '''
    SELECT count(foo 1);
    ''',
    '''
    SELECT ((count(foo 1)));
    ''',
    '''
    SELECT count(SELECT 1);
    ''',
    '''
    SELECT (
        # reserved keywords
        select := 2
    );
    ''',
    '''
    SELECT INTROSPECT tuple<int64>;
    ''',
    '''
    (SELECT User.name) OFFSET 2;
    ''',
    '''
    default::Movie.name;
    ''',
    '''
    WITH MODULE welp
    CREATE DATABASE sample;
    ''',
    '''
    INSERT Foo FILTER Foo.bar = 42;
    ''',
    '''
    start migration to {
      module default {
        type Hello extending MetaHello {
          property platform_fee_percentage: int16 {
            constrant exclusive {
              errmessage := "asxasx";
            }
          }
          required property blah := .bleh - .bloh - .blih;
        }
      }
    }
    ''',
    '''
    SELECT __type__;
    ''',
    '''
    INSERT Foo GROUP BY Foo.bar;
    ''',
    '''
    WITH MODULE welp
    CREATE DATABASE sample;
    ''',
    '''
    WITH MODULE welp
    DROP DATABASE sample;
    ''',
    '''
    SELECT (1, a := 2);
    ''',
    '''
        SELECT Issue{
            name,
            related_to *$var,
        };
    ''',
    '''
        SELECT Issue{
            name,
            related_to *5,
        };
    ''',
    '''
        START MIGRATION TO BadLang $$type Foo$$;
    ''',
    '''
        SELECT Issue{
            name,
            related_to *5,
        };
    ''',
    '''sdl# comment
    '''
]
