#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

import click
import collections
import json
import os
import sys
import textwrap

from edb.tools.edb import edbcommands
from edb.pgsql.parser.parser import pg_parse
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import codegen as qlcodegen
from edb.common import assert_data_shape


BROKEN = []


def die(msg):
    print(f'FATAL: {msg}', file=sys.stderr)
    sys.exit(1)


def record_broken(*args):
    '''Record functions or operators that are broken by this conversion.

    Some functions cannot be reflected due to unusual/internal parameters.
    It's OK to omit them, but a list should be kept of all such omitted
    functions and reasons.
    '''

    BROKEN.append(args)


def sql_to_eqltype(ret):
    for nameobj in ret['names']:
        name = nameobj['String']['sval']
        if name == 'pg_catalog':
            continue
        atype = None
        pgtype = None

        match name:
            case 'geometry':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='geometry',
                        module='ext::postgis',
                    ),
                )
                break
            case 'geography':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='geography',
                        module='ext::postgis',
                    ),
                )
                break
            case 'box2d' | 'box2df':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='box2d',
                        module='ext::postgis',
                    ),
                )
                break
            case 'box3d':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='box3d',
                        module='ext::postgis',
                    ),
                )
                break
            case 'text':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='str',
                        module='std',
                    ),
                )
                break
            case 'json':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='json',
                        module='std',
                    ),
                )
                break
            case 'float8':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='float64',
                        module='std',
                    ),
                )
                break
            case 'float4':
                # Use float64 for the type because that's the default for
                # EdgeDB, but also record the original type for casting to the
                # postgres function call.
                pgtype = 'float4'
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='float64',
                        module='std',
                    ),
                )
                break
            case 'int8':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='int64',
                        module='std',
                    ),
                )
                break
            case 'int4':
                # Use int64 for the type because that's the default for
                # EdgeDB, but also record the original type for casting to the
                # postgres function call.
                pgtype = 'int4'
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='int64',
                        module='std',
                    ),
                )
                break
            case 'int2':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='int16',
                        module='std',
                    ),
                )
                break
            case 'bool':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='bool',
                        module='std',
                    ),
                )
                break
            case 'bytea':
                atype = qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='bytes',
                        module='std',
                    ),
                )
                break
            case _:
                raise Exception(
                    f'unknown type: {name!s}')

    if 'arrayBounds' in ret:
        # Make it an array
        atype = qlast.TypeName(
            maintype=qlast.ObjectRef(name='array'),
            subtypes=[atype],
        )
        pgtype = f'{name}[]'

    return atype, pgtype


def get_expr(param):
    if param is None:
        return None
    else:
        cast = None
        if 'A_Const' in param:
            p = param['A_Const']
        elif 'TypeCast' in param:
            p = param['TypeCast']['arg']['A_Const']
            cast, _ = sql_to_eqltype(param['TypeCast']['typeName'])

        if 'ival' in p:
            val = p['ival'].get('ival', 0)
            expr = qlast.Constant.integer(val)
        elif 'fval' in p:
            val = p['fval'].get('fval', '0.0')
            expr = qlast.Constant(
                value=val,
                kind=qlast.ConstantKind.FLOAT,
            )
        elif 'sval' in p:
            val = p['sval'].get('sval', '')
            expr = qlast.Constant.string(val)
        elif 'boolval' in p:
            val = p['boolval'].get('boolval', False)
            expr = qlast.Constant.boolean(val)
        elif p.get('isnull'):
            expr = qlast.Set(elements=[])

        if cast:
            return qlast.TypeCast(
                type=cast,
                expr=expr,
            )
        else:
            return expr

    return None


def get_params(params, is_strict):
    res = []
    callsig = []
    needs_adapter = False

    if params:
        for i, p in enumerate(params):
            default = get_expr(p['FunctionParameter'].get('defexpr'))
            # optional arguments are either because function is not strict or
            # because the default is null
            is_strict = is_strict and not isinstance(default, qlast.Set)
            pname = p['FunctionParameter'].get('name', f'a{i}')
            ptype, pgtype = sql_to_eqltype(p['FunctionParameter']['argType'])

            if pgtype is not None:
                needs_adapter = True
                callsig.append(f'"{pname}"::{pgtype}')
            else:
                callsig.append(f'"{pname}"')

            eqlp = qlast.FuncParam(
                name=pname,
                kind=qltypes.ParameterKind.PositionalParam,
                type=ptype,
                default=default,
                typemod=qltypes.TypeModifier.SingletonType
                        if is_strict else
                        qltypes.TypeModifier.OptionalType,
            )
            res.append(eqlp)

    return res, callsig, needs_adapter


def convert_function_sig(eqlname, fname, func, is_strict, adapt_fns):
    params, callsig, needs_adapter = get_params(
        func.get('parameters'), is_strict)

    if needs_adapter:
        code = qlast.FunctionCode(
            language=qlast.Language.SQL,
            code=f'SELECT {fname}({", ".join(callsig)})',
        )
        # Need to review all overloaded functions to match the way they are
        # called with SQL expr instead of using PG function directly.
        adapt_fns.add(eqlname)
    else:
        code = qlast.FunctionCode(
            language=qlast.Language.SQL,
            from_function=fname,
        )

    return params, code


def screen_name(name):
    # We want to rename our EdgeDB functions, sometimes to drop the "st"
    # prefix, other times because we have specific naming convention for
    # certain types of functions.
    if name.startswith('st_'):
        name = name[3:]

    if name in {'geometry', 'geography', 'box2d', 'box3d'}:
        name = f'to_{name}'
    elif name.startswith('3d'):
        # It actually starts with 'st_3d', but we just cleaned the 'st_'
        name = f'{name[2:]}3d'

    return name


def clean_up_sqlfn(defn):
    if isinstance(defn, list):
        res = [clean_up_sqlfn(val) for val in defn]
    elif isinstance(defn, dict):
        res = {
            key: clean_up_sqlfn(val) for key, val in defn.items()
            if key not in {'funcname', 'location'}
        }
    else:
        res = defn

    return res


def compare_sql_defs(def0, def1):
    # Compare two parsed out SQL funciton definitions.
    #
    # If the functions are the same except for name and location values, they
    # are considered identical and only one of them needs to be ported.

    try:
        assert_data_shape.assert_data_shape(
            clean_up_sqlfn(def0),
            clean_up_sqlfn(def1),
            Exception
        )
    except Exception:
        return False
    else:
        return True


def get_options(func):
    volatility = 'Volatile'
    is_strict = False
    is_window = False

    for opt in func['options']:
        if opt['DefElem']['defname'] == 'volatility':
            volatility = opt['DefElem']['arg']['String']['sval'].capitalize()
        elif opt['DefElem']['defname'] == 'strict':
            is_strict = opt['DefElem']['arg']['Boolean']['boolval']
        elif opt['DefElem']['defname'] == 'window':
            is_window = opt['DefElem']['arg']['Boolean']['boolval']

    return volatility, is_strict, is_window


def get_comment(name, func, comments):
    # Given a SQL function find the corresponding comment based on signature.
    for comm in comments.get(name, []):

        if compare_sql_defs(
            func.get('parameters', []),
            comm['object']['ObjectWithArgs'].get('objfuncargs', []),
        ):
            return comm['comment']

    return None


def parse_postgis_extension(
    path, functions, aggregates, comments, aggcomments, operators
):
    for root, _dirs, files in os.walk(path):
        for name in files:
            if name in {'postgis--3.4.2.sql'}:
                with open(os.path.join(root, name), mode='rt') as f:
                    sql_query = ''.join(
                        line for line in f.readlines()
                        if not line.startswith('\\')
                    )
                    ast_json = pg_parse(bytes(sql_query, encoding="UTF8"))
                    for code in json.loads(ast_json)['stmts']:
                        if 'stmt' in code:
                            stmt = code['stmt']

                            if defn := stmt.get('DefineStmt'):
                                if defn['kind'] == 'OBJECT_OPERATOR':
                                    operators.append(defn)
                                elif defn['kind'] == 'OBJECT_AGGREGATE':
                                    _el = defn['defnames'][0]
                                    name = _el['String']['sval']
                                    aggregates[name].append(defn)

                            elif func := stmt.get('CreateFunctionStmt'):
                                name = func['funcname'][0]['String']['sval']
                                functions[name].append(func)

                            elif comm := stmt.get('CommentStmt'):
                                if comm['objtype'] == 'OBJECT_FUNCTION':
                                    _o = comm['object']['ObjectWithArgs']
                                    name = _o['objname'][0]['String']['sval']
                                    comments[name].append(comm)
                                elif comm['objtype'] == 'OBJECT_AGGREGATE':
                                    _o = comm['object']['ObjectWithArgs']
                                    name = _o['objname'][0]['String']['sval']
                                    aggcomments[name].append(comm)


def generate_eqlop(operators, functions):
    eqlop = []
    for op in operators:
        name = op['defnames'][0]['String']['sval']
        if name not in {'<', '<=', '>', '>=', '='}:
            try:
                for defn in op['definition']:

                    el = defn['DefElem']
                    if el['defname'] == 'leftarg':
                        ltype, _ = sql_to_eqltype(el['arg']['TypeName'])
                    elif el['defname'] == 'rightarg':
                        rtype, _ = sql_to_eqltype(el['arg']['TypeName'])
                    elif el['defname'] == 'procedure':
                        _el = el['arg']['TypeName']['names'][0]
                        fname = _el['String']['sval']

                # For most operators there is only one function, but when there
                # are multiple they have the same return type.
                func = functions[fname][0]
                volatility, _, _ = get_options(func)

                eqlname = fname
                if eqlname.startswith('geo'):
                    eqlname = eqlname.replace("geometry_", "") \
                                     .replace("geography_", "")
                eqlname = f'op_{eqlname}'

                ef = qlast.CreateFunction(
                    name=qlast.ObjectRef(
                        name=eqlname,
                        module='ext::postgis',
                        itemclass=qltypes.SchemaObjectClass.FUNCTION,
                    ),
                    params=[
                        qlast.FuncParam(
                            name='a',
                            kind=qltypes.ParameterKind.PositionalParam,
                            type=ltype,
                            typemod=qltypes.TypeModifier.SingletonType,
                        ),
                        qlast.FuncParam(
                            name='b',
                            kind=qltypes.ParameterKind.PositionalParam,
                            type=rtype,
                            typemod=qltypes.TypeModifier.SingletonType,
                        ),
                    ],
                    returning=sql_to_eqltype(func['returnType'])[0],
                    code=qlast.FunctionCode(
                        language=qlast.Language.SQL,
                        code=f'SELECT a {name} b',
                    ),
                    commands=[
                        qlast.SetField(
                            name='volatility',
                            value=qlast.Constant.string(volatility),
                        ),
                        qlast.SetField(
                            name='impl_is_strict',
                            value=qlast.Constant.boolean(False),
                        ),
                        qlast.SetField(
                            name='prefer_subquery_args',
                            value=qlast.Constant.boolean(True),
                        ),
                    ],
                )
                eqlop.append(ef)

            except Exception as e:
                record_broken(name, op, e)

    return eqlop


def generate_eqlfunc(functions, comments):
    eqlfunc = []
    adapt_fns = set()

    for key, func_list in functions.items():
        for func in func_list:
            try:
                volatility, is_strict, is_window = get_options(func)

                if is_window:
                    # skip window functions for now
                    continue

                eqlname = screen_name(key)
                commands = [
                    qlast.SetField(
                        name='volatility',
                        value=qlast.Constant.string(volatility),
                    ),
                    qlast.SetField(
                        name='force_return_cast',
                        value=qlast.Constant.boolean(True),
                    ),
                ]
                comment = get_comment(key, func, comments)
                if comment:
                    commands.append(qlast.CreateAnnotationValue(
                        name=qlast.ObjectRef(
                            name='description',
                        ),
                        value=qlast.Constant.string(comment),
                    ))

                if not is_strict:
                    commands.append(qlast.SetField(
                        name='impl_is_strict',
                        value=qlast.Constant.boolean(False),
                    ))

                params, code = convert_function_sig(
                    eqlname, key, func, is_strict, adapt_fns)
                rettype, _ = sql_to_eqltype(func['returnType'])

                if (
                    eqlname in {'to_geometry', 'to_geography'}
                    and len(params) > 1
                ):
                    # We only care about converter functions that take a
                    # single argument here. Other casting functions take
                    # typemod indicating a geometry or geography subtype which
                    # we don't currently support. If and when we would support
                    # that, we'd expose them in a custom way using enums
                    # rather than integer codes.
                    continue

                ef = qlast.CreateFunction(
                    name=qlast.ObjectRef(
                        name=eqlname,
                        module='ext::postgis',
                        itemclass=qltypes.SchemaObjectClass.FUNCTION,
                    ),
                    params=params,
                    returning=rettype,
                    returning_typemod=qltypes.TypeModifier.SingletonType
                                      if is_strict else
                                      qltypes.TypeModifier.OptionalType,
                    code=code,
                    commands=commands,
                )
                eqlfunc.append(ef)
            except Exception as e:
                record_broken(key, func, e)

    return eqlfunc, adapt_fns


def generate_eqlagg(aggregates, functions, comments):
    eqlagg = []
    for key, agg_list in aggregates.items():
        for func in agg_list:
            try:
                eqlname = f'{screen_name(key)}_agg'

                comment = get_comment(key, func, comments)
                if not comment:
                    continue

                commands = [
                    qlast.SetField(
                        name='volatility',
                        value=qlast.Constant.string('Immutable'),
                    ),
                    qlast.SetField(
                        name='force_return_cast',
                        value=qlast.Constant.boolean(True),
                    ),
                    qlast.CreateAnnotationValue(
                        name=qlast.ObjectRef(
                            name='description',
                        ),
                        value=qlast.Constant.string(comment),
                    ),
                ]

                params, _, _ = get_params(
                    func['args'][0]['List']['items'], True)
                params[0].typemod = qltypes.TypeModifier.SetOfType
                code = qlast.FunctionCode(
                    language=qlast.Language.SQL,
                    from_function=key,
                )

                for el in func['definition']:
                    defel = el['DefElem']
                    if defel['defname'] == 'finalfunc':
                        _el = defel['arg']['TypeName']['names'][0]
                        ffname = _el['String']['sval']
                        ffunc = functions[ffname][0]
                        rettype, _ = sql_to_eqltype(ffunc['returnType'])
                        break

                ef = qlast.CreateFunction(
                    name=qlast.ObjectRef(
                        name=eqlname,
                        module='ext::postgis',
                        itemclass=qltypes.SchemaObjectClass.FUNCTION,
                    ),
                    params=params,
                    returning=rettype,
                    returning_typemod=qltypes.TypeModifier.OptionalType,
                    code=code,
                    commands=commands,
                )
                eqlagg.append(ef)

            except Exception as e:
                record_broken(key, func, e)

    return eqlagg


def main(show_broken=False):
    functions = collections.defaultdict(list)
    aggregates = collections.defaultdict(list)
    comments = collections.defaultdict(list)
    aggcomments = collections.defaultdict(list)
    operators = []

    parse_postgis_extension(
        'build/postgres/install/share/extension/',
        functions, aggregates, comments, aggcomments, operators,
    )
    eqlop = generate_eqlop(operators, functions)

    # remove functions corresponding to operators
    for op in operators:
        name = op['defnames'][0]['String']['sval']

        for defn in op['definition']:
            if (el := defn['DefElem'])['defname'] == 'procedure':
                fname = el['arg']['TypeName']['names'][0]['String']['sval']
                if fname in functions:
                    del functions[fname]
                break

    # clean up some functions
    for name in list(functions.keys()):
        if (
            name in {
                # Functions that are reflected manually or otherwise have
                # special handling.
                'equals', 'st_letters', 'json', 'jsonb',

                # Deprecated and duplicated
                'geomfromewkb', 'geomfromewkt',

                # Functions involving columns, tables or subtypes.
                'st_geometrytype', 'populate_geometry_columns',
                'st_findextent', 'st_estimatedextent',
                'postgis_extensions_upgrade',

                # Row locking functions that cannot just be reflected as is.
                'addauth', 'checkauth', 'disablelongtransactions',
                'enablelongtransactions', 'lockrow', 'unlockrows',
            }
            or name.startswith('_')
            or name.endswith('_in')
            or name.endswith('_out')
            or name.endswith('_send')
            or name.endswith('_recv')
            or name.endswith('_analyze')
        ):
            # skip functions we're not reflecting
            del functions[name]

    eqlfunc, adapt_fns = generate_eqlfunc(functions, comments)
    eqlagg = generate_eqlagg(aggregates, functions, aggcomments)

    if show_broken:
        # output all the broken functions instead of the extension
        for name, sqlfunc, e in BROKEN:
            match show_broken:
                case 'names':
                    print(name)
                case 'all':
                    print(name)
                    print(sqlfunc)
                    print(e, '\n')
        sys.exit(1)

    # Review all generated functions to make sure that the way they are
    # implemented is consistent across overloaded variants.
    for func in eqlfunc:
        if func.name.name in adapt_fns:
            code = func.code
            if code.from_function is not None:
                # fix this by rewriting the call as 'SELECT ...'
                sig = ', '.join(f'"{p.name}"' for p in func.params)
                code.code = f'SELECT {code.from_function}({sig})'
                code.from_function = None

    with open('edb/tools/postgis.template.edgeql', mode='rt') as tf:
        for line in tf.readlines():
            match line:
                case '### REFLECT: OPERATORS\n':
                    text = (
                        f'# total operators: {len(eqlop)}\n'
                        +
                        '#' * 50
                        +
                        '\n'
                    )
                    print(textwrap.indent(text, '    '))

                    for ef in eqlop:
                        print(textwrap.indent(
                            "# Postgres only manages to inline this function "
                            "if it isn't marked\n"
                            "# strict, and we want it to be inlined so that "
                            "indexes work with it.",
                            '    ',
                        ))
                        code = qlcodegen.generate_source(
                            ef, pretty=True
                        ).replace('\n;', ';\n')
                        print(textwrap.indent(f'{code};\n', '    '))

                case '### REFLECT: FUNCTIONS\n':
                    text = (
                        f'# total functions: {len(eqlfunc)}\n'
                        +
                        '#' * 50
                        +
                        '\n'
                    )
                    print(textwrap.indent(text, '    '))

                    for ef in eqlfunc:
                        code = qlcodegen.generate_source(
                            ef, pretty=True).replace('\n;', ';\n')
                        print(textwrap.indent(f'{code};\n', '    '))

                case '### REFLECT: AGGREGATES\n':
                    text = (
                        f'# total aggregates: {len(eqlagg)}\n'
                        +
                        '#' * 50
                        +
                        '\n'
                    )
                    print(textwrap.indent(text, '    '))

                    for ef in eqlagg:
                        code = qlcodegen.generate_source(
                            ef, pretty=True).replace('\n;', ';\n')
                        print(textwrap.indent(f'{code};\n', '    '))

                case _:
                    print(line, end='')


@edbcommands.command('gen-ext-postgis')
@click.option('--show-broken',
              type=click.Choice(['names', 'all'], case_sensitive=False))
def gen_ext_postgis(*, show_broken):
    """Generate ext::postgis extension file based on the installed PostGIS.
    """
    try:
        main(show_broken=show_broken)
    except Exception as ex:
        die(str(ex))
