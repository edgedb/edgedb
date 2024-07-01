import json
import os
import textwrap

from edb.pgsql.parser.parser import pg_parse
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import codegen as qlcodegen
from edb.common import assert_data_shape

from collections import defaultdict


functions = defaultdict(list)
comments = defaultdict(list)
operators = []
eqlfunc = []
eqlop = []
adapt_fns =set()
broken = []


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
            # optional arguments are either because funciton is not strict or
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


def convert_function_sig(eqlname, fname, func, is_strict):
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
    except Exception as e:
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


def get_comment(name, func):
    # Given a SQL function find the corresponding comment based on signature.
    for comm in comments.get(name, []):

        if compare_sql_defs(
            func.get('parameters',[]),
            comm['object']['ObjectWithArgs'].get('objfuncargs', []),
        ):
            return comm['comment']

    return None


for root, dirs, files in os.walk('build/postgres/install/share/extension/'):
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

                        if op := stmt.get('DefineStmt'):
                            if op['kind'] == 'OBJECT_OPERATOR':
                                operators.append(op)

                        elif func := stmt.get('CreateFunctionStmt'):
                            name = func['funcname'][0]['String']['sval']
                            functions[name].append(func)

                        elif comm := stmt.get('CommentStmt'):
                            if comm['objtype'] == 'OBJECT_FUNCTION':
                                name = comm['object']['ObjectWithArgs'] \
                                       ['objname'][0]['String']['sval']
                                comments[name].append(comm)



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
                    fname = el['arg']['TypeName']['names'][0]['String']['sval']

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
                        name='prefer_subquery_args',
                        value=qlast.Constant.boolean(True),
                    ),
                ],
            )
            eqlop.append(ef)

        except Exception as e:
            broken.append((name, op, e))


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
            'equals', 'st_geometrytype', 'geomfromewkb', 'geomfromewkt',
            'populate_geometry_columns',
        }
        or name.startswith('_')
        or name.endswith('_in')
        or name.endswith('_out')
        or name.endswith('_send')
        or name.endswith('_recv')
        or name.endswith('_analyze')
    ):
        # skip obvious helpers
        del functions[name]


for key, func_list in functions.items():
    for func in func_list:
        try:
            volatility, is_strict, is_window = get_options(func)

            if is_window:
                # skip window functions for now
                continue

            eqlname = screen_name(key)
            commands=[
                qlast.SetField(
                    name='volatility',
                    value=qlast.Constant.string(volatility),
                ),
                qlast.SetField(
                    name='force_return_cast',
                    value=qlast.Constant.boolean(True),
                ),
            ]
            comment = get_comment(key, func)
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

            params, code = convert_function_sig(eqlname, key, func, is_strict)
            rettype, _ = sql_to_eqltype(func['returnType'])

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
            broken.append((key, func, e))


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


print(f'# total errors: {len(broken)}')
print(f'# total operators: {len(eqlop)}')
print('#'*50, flush=True)
for ef in eqlop:
    code = qlcodegen.generate_source(ef, pretty=True).replace('\n;', ';\n')
    print(f'{code};\n')

print(f'# total functions: {len(eqlfunc)}')
print('#'*50, flush=True)
for ef in eqlfunc:
    code = qlcodegen.generate_source(ef, pretty=True).replace('\n;', ';\n')
    if 'array<ext' in code:
        # currently arrays of geometry are causing an issue:
        #
        # edb.errors.InvalidValueError: cannot determine OID of EdgeDB type
        # 'b855d9a4-eb15-5850-9a93-2fd6e35197ae'
        #
        # So we comment them out
        print('# FIXME: array<geometry> is causing an issue ', flush=True)
        code = textwrap.indent(code, '# ')
        print(f'{code};\n')

    else:
        print(f'{code};\n')
