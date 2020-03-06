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


"""EdgeQL to IR compiler.

The purpose of this compilation phase is to produce a canonical, self-contained
representation of an EdgeQL expression, aka the IR.  The validity of the
expression and other schema-level checks and resolutions happen at this stage.
Once the IR generation is successful, the expression is considered valid.

The finalized IR consists of two tree structures: the expression tree and
the scope tree.

The *expression tree* is, essentially, another AST form that generally
resembles the overall shape of the original EdgeQL AST annotated with type
information and other metadata that is necessary to compile the IR into the
backend query language.  The *scope tree* tracks the visibility of variables
and determines how the aggregation functions are arranged in the expression.

Every EdgeQL expression is essentially a giant functional map-reduce
construct, or, Pythonically, a bunch of nested set comprehensions.
In those terms, the expression tree encodes expressions inside comprehensions,
and the scope tree determines how the comprehensions are nested, and at which
comprehension level the variables are defined.

The :mod:`ir.ast` and the :mod:`ir.scopetree` modules have more comments on
the organization of the IR expression and scope trees, correspondingly.

Operation
---------

The compiler has several entry points, are all in this file.  Each entry
point sets the compilation context and then calls the generic compilation
dispatch.  The compilation process is a straightforward EdgeQL AST traversal,
where most AST nodes have a dedicated handler function, and the routing is
done by singledispatch based on the AST node type.

Context
-------

The compilation context object is passed to the vast majority of the compiler
functions and contains the information necessary to correctly process an AST
node in a given situation.  It is organized as a stack that resembles a
ChainMap, albeit the elements are objects instead of dicts, and the chaining
logic is controlled by the context itself.  See context.py for details.

Organization
------------

The compiler code is organized into the following modules (in rough order
of control flow):

__init__.py
    This file, contains compiler entry points that initialize
    the compilation context and call into compilation dispatch.

stmt.py
    Handlers for statement expressions, like ``SELECT``, ``INSERT``.

expr.py
    Handlers for the majority of expressions that aren't statements or
    that are handled elsewhere.

func.py
    Handlers for function calls and operator expressions.

casts.py
    Handlers for type cast expressions.

clauses.py
    Handlers for common statement clauses like ``FILTER`` and ``ORDER BY``.

polyres.py
    Logic for function, operator, and cast lookup via multiple dispatch
    and generic type specialization.

config.py
    Handlers for ``CONFIGURE`` commands.

setgen.py
    Functions to generate ``ir.ast.Set`` nodes and process path expressions.

viewgen.py
    Functions that process shape expressions into view types.

typegen.py
    Helpers for type expressions.

context.py
    Compilation context definition.

stmtctx.py
    Functions to set up the overall compilation context as well as finalize
    the result IR.

pathctx.py
    PathId and scope helpers.

schemactx.py
    Helpers that interface with the schema, such as object lookup and
    derivation.

astutils.py
    Various helpers for EdgeQL AST analysis.

dispatch.py
    Compiler singledispatch decorator (separate module for ease of import).

"""


from __future__ import annotations
from typing import *

from edb import errors

from edb.edgeql import parser as ql_parser
from edb.edgeql import qltypes

from edb.common import debug
from edb.common import markup  # NOQA

from edb.schema import functions as s_func
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import typeutils as irtyputils

from . import dispatch
from . import inference
from . import setgen
from . import stmtctx

from .config import get_config_type_shape  # NOQA

from . import expr as _expr_compiler  # NOQA
from . import config as _config_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA

if TYPE_CHECKING:
    from edb.schema import name as s_name
    from edb.schema import objects as s_obj
    from edb.schema import schema as s_schema


def compile_ast_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    anchors: Optional[
        Mapping[
            irast.AnchorsKeyType,
            irast.AnchorsValueType
        ]
    ] = None,
    path_prefix_anchor: Optional[irast.AnchorsKeyType] = None,
    singletons: Sequence[s_types.Type] = (),
    func_params: Optional[s_func.ParameterLikeList] = None,
    result_view_name: Optional[s_name.SchemaName] = None,
    derived_target_module: Optional[str] = None,
    parent_object_type: Optional[s_obj.ObjectMeta] = None,
    implicit_limit: int = 0,
    implicit_id_in_shapes: bool = False,
    implicit_tid_in_shapes: bool = False,
    schema_view_mode: bool = False,
    session_mode: bool = False,
    disable_constant_folding: bool = False,
    json_parameters: bool = False,
    allow_generic_type_output: bool = False,
) -> irast.Command:
    """Compile given EdgeQL AST into EdgeDB IR.

    This is the normal compiler entry point.  It assumes that *tree*
    represents a complete statement.

    Args:
        tree:
            EdgeQL AST.

        schema:
            Schema instance.  Must contain definitions for objects
            referenced by the AST *tree*.

        modaliases:
            Module name resolution table.  Useful when this EdgeQL
            expression is part of some other construct, such as a
            DDL statement.

        anchors:
            Predefined symbol table.  Maps identifiers
            (or ``qlast.SpecialAnchor`` instances) to specified
            schema objects or IR fragments.

        path_prefix_anchor:
            Symbol name used to resolve the prefix of abbreviated
            path expressions by default.  The symbol must be present
            in *anchors*.

        singletons:
            An optional set of schema types that should be treated
            as singletons in the context of this compilation.

        func_params:
            When compiling a function body, specifies function parameter
            definitions.

        result_view_name:
            Optionally defines the name of the topmost generated view type.
            Useful when compiling schema views.

        derived_target_module:
            The name of the module where derived types and pointers should
            be placed.  When compiling a schema view, this would be the
            name of the module where the view is defined.  By default,
            the special ``__derived__`` module is used.

        parent_object_type:
            Optionaly specifies the class of the schema object, in the
            context of which this expression is compiled.  Used in schema
            definitions.

        implicit_limit:
            If set to a non-zero integer value, this will be injected
            as an implicit `LIMIT` clause into each read query.

        implicit_id_in_shapes:
            Whether to include object id property in shapes by default.

        implicit_tid_in_shapes:
            Whether to implicitly include object type id in shapes as
            the ``__tid__`` computable.

        schema_view_mode:
            When compiling a schema view, set this to ``True``.

        session_mode:
            When ``True``, assumes that the expression is compiled in
            the presence of a persistent database session.  Otherwise,
            the use of functions and other constructs that require a
            persistent session will trigger an error.

        disable_constant_folding:
            When ``True``, the compile-time evaluation and substitution
            of constant expressions is disabled.

        json_parameters:
            When ``True``, the argument values are assumed to be in JSON
            format.

        allow_generic_type_output:
            If ``True``, allows the expression to return a generic type.
            By default, expressions must resolve into concrete types.

    Returns:
        An instance of :class:`ir.ast.Command`.  Most frequently, this
        would be an instance of :class:`ir.ast.Statement`.
    """

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL AST')
        debug.dump(tree, schema=schema)

    ctx = stmtctx.init_context(
        schema=schema,
        anchors=anchors,
        singletons=singletons,
        modaliases=modaliases,
        func_params=func_params,
        derived_target_module=derived_target_module,
        result_view_name=result_view_name,
        implicit_limit=implicit_limit,
        implicit_id_in_shapes=implicit_id_in_shapes,
        implicit_tid_in_shapes=implicit_tid_in_shapes,
        schema_view_mode=schema_view_mode,
        disable_constant_folding=disable_constant_folding,
        json_parameters=json_parameters,
        session_mode=session_mode,
        allow_generic_type_output=allow_generic_type_output,
        parent_object_type=parent_object_type,
    )

    if path_prefix_anchor is not None:
        assert anchors is not None
        path_prefix = anchors[path_prefix_anchor]
        assert isinstance(path_prefix, s_types.Type)
        ctx.partial_path_prefix = setgen.class_set(path_prefix, ctx=ctx)
        ctx.partial_path_prefix.anchor = path_prefix_anchor
        ctx.partial_path_prefix.show_as_anchor = path_prefix_anchor

    ir_set = dispatch.compile(tree, ctx=ctx)
    ir_expr = stmtctx.fini_expression(ir_set, ctx=ctx)

    if ctx.env.query_parameters:
        first_argname = next(iter(ctx.env.query_parameters))
        if first_argname.isdecimal():
            args_decnames = {int(arg) for arg in ctx.env.query_parameters}
            args_tpl = set(range(len(ctx.env.query_parameters)))
            if args_decnames != args_tpl:
                missing_args = args_tpl - args_decnames
                missing_args_repr = ', '.join(f'${a}' for a in missing_args)
                raise errors.QueryError(
                    f'missing {missing_args_repr} positional argument'
                    f'{"s" if len(missing_args) > 1 else ""}')

    if debug.flags.edgeql_compile:
        debug.header('Scope Tree')
        if ctx.path_scope is not None:
            print(ctx.path_scope.pdebugformat())
        else:
            print('N/A')
        debug.header('EdgeDB IR')
        debug.dump(ir_expr, schema=getattr(ir_expr, 'schema', None))

    return ir_expr


def compile_ast_fragment_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    anchors: Optional[
        Mapping[irast.AnchorsKeyType, irast.AnchorsValueType]
    ] = None,
    path_prefix_anchor: Optional[irast.AnchorsKeyType] = None,
) -> irast.Statement:
    """Compile given EdgeQL AST fragment into EdgeDB IR.

    Unlike :func:`~compile_ast_to_ir` above, this does not assume
    that the AST *tree* is a complete statement.  The expression
    doesn't even have to resolve to a specific type.

    Args:
        tree:
            EdgeQL AST fragment.

        schema:
            Schema instance.  Must contain definitions for objects
            referenced by the AST *tree*.

        modaliases:
            Module name resolution table.  Useful when this EdgeQL
            expression is part of some other construct, such as a
            DDL statement.

        anchors:
            Predefined symbol table.  Maps identifiers
            (or ``qlast.SpecialAnchor`` instances) to specified
            schema objects or IR fragments.

        path_prefix_anchor:
            Symbol name used to resolve the prefix of abbreviated
            path expressions by default.  The symbol must be present
            in *anchors*.

    Returns:
        An instance of :class:`ir.ast.Statement`.
    """
    ctx = stmtctx.init_context(
        schema=schema, anchors=anchors, modaliases=modaliases)
    if path_prefix_anchor is not None:
        assert anchors is not None
        path_prefix = anchors[path_prefix_anchor]
        assert isinstance(path_prefix, s_types.Type)
        ctx.partial_path_prefix = setgen.class_set(path_prefix, ctx=ctx)
        ctx.partial_path_prefix.anchor = path_prefix_anchor
        ctx.partial_path_prefix.show_as_anchor = path_prefix_anchor

    ir_set = dispatch.compile(tree, ctx=ctx)

    result_type: Optional[s_types.Type]
    try:
        result_type = inference.infer_type(ir_set, ctx.env)
    except errors.QueryError:
        # Not all fragments can be resolved into a concrete type,
        # that's OK.
        result_type = None

    return irast.Statement(expr=ir_set, schema=ctx.env.schema,
                           stype=result_type)


def evaluate_to_python_val(
    expr: str,
    schema: s_schema.Schema,
    *,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> Any:
    """Evaluate the given EdgeQL string as a constant expression.

    Args:
        expr:
            EdgeQL expression as a string.

        schema:
            Schema instance.  Must contain definitions for objects
            referenced by *expr*.

        modaliases:
            Module name resolution table.  Useful when this EdgeQL
            expression is part of some other construct, such as a
            DDL statement.

    Returns:
        The result of the evaluation as a Python value.

    Raises:
        If the expression is not constant, or is otherwise not supported by
        the const evaluator, the function will raise
        :exc:`ir.staeval.UnsupportedExpressionError`.
    """
    tree = ql_parser.parse_fragment(expr)
    return evaluate_ast_to_python_val(tree, schema, modaliases=modaliases)


def evaluate_ast_to_python_val(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> Any:
    """Evaluate the given EdgeQL AST as a constant expression.

    Args:
        tree:
            EdgeQL AST.

        schema:
            Schema instance.  Must contain definitions for objects
            referenced by AST *tree*.

        modaliases:
            Module name resolution table.  Useful when this EdgeQL
            expression is part of some other construct, such as a
            DDL statement.

    Returns:
        The result of the evaluation as a Python value.

    Raises:
        If the expression is not constant, or is otherwise not supported by
        the const evaluator, the function will raise
        :exc:`ir.staeval.UnsupportedExpressionError`.
    """
    ir = compile_ast_fragment_to_ir(tree, schema, modaliases=modaliases)
    return ireval.evaluate_to_python_val(ir.expr, schema=ir.schema)


def compile_func_to_ir(
    func: s_func.Function,
    schema: s_schema.Schema,
) -> irast.Statement:
    """Compile an EdgeQL function into EdgeDB IR.

    Args:
        func:
            A function object.

        schema:
            A schema instance where the function is defined.

    Returns:
        An instance of :class:`ir.ast.Statement` representing the
        function body.
    """
    if debug.flags.edgeql_compile:
        debug.header('EdgeQL Function')
        debug.print(func.get_code(schema))

    code = func.get_code(schema)
    assert code is not None
    trees = ql_parser.parse_block(code + ';')
    if len(trees) != 1:
        raise errors.InvalidFunctionDefinitionError(
            'functions can only contain one statement')

    tree = trees[0]

    param_anchors, param_aliases = get_param_anchors_for_callable(
        func.get_params(schema), schema,
        inlined_defaults=func.has_inlined_defaults(schema))

    tree.aliases.extend(param_aliases)

    ir = compile_ast_to_ir(
        tree, schema,
        anchors=param_anchors,
        func_params=func.get_params(schema),
        # the body of a session_only function can contain calls to
        # other session_only functions
        session_mode=func.get_session_only(schema),
    )

    assert isinstance(ir, irast.Statement)

    return_type = func.get_return_type(schema)
    if (not ir.stype.issubclass(schema, return_type)
            and not ir.stype.implicitly_castable_to(return_type, schema)):
        raise errors.InvalidFunctionDefinitionError(
            f'return type mismatch in function declared to return '
            f'{return_type.get_verbosename(schema)}',
            details=f'Actual return type is '
                    f'{ir.stype.get_verbosename(schema)}',
            context=tree.context,
        )

    return_typemod = func.get_return_typemod(schema)
    if (return_typemod is not qltypes.TypeModifier.SET_OF
            and ir.cardinality is qltypes.Cardinality.MANY):
        raise errors.InvalidFunctionDefinitionError(
            f'return cardinality mismatch in function declared to return '
            f'a singleton',
            details=f'Function may return a set with more than one element.',
            context=tree.context,
        )

    return ir


def compile_constant_tree_to_ir(
    const: qlast.BaseConstant,
    schema: s_schema.Schema,
    *,
    styperef: Optional[irast.TypeRef] = None,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> irast.Expr:
    """Compile an EdgeQL constant into an IR ConstExpr.

    Args:
        const:
            An EdgeQL AST representing a constant.

        schema:
            A schema instance.  Must contain the definition of the
            constant type.

        styperef:
            Optionally overrides an IR type descriptor for the returned
            ConstExpr.  If not specified, the inferred type of the constant
            is used.

        modaliases:
            Module name resolution table.  Useful when this EdgeQL
            expression is part of some other construct, such as a
            DDL statement.

    Returns:
        An instance of :class:`ir.ast.ConstExpr` representing the
        constant.
    """
    ctx = stmtctx.init_context(
        schema=schema,
        modaliases=modaliases,
    )

    if not isinstance(const, qlast.BaseConstant):
        raise ValueError(f'unexpected input: {const!r} is not a constant')

    ir_set = dispatch.compile(const, ctx=ctx)
    assert isinstance(ir_set, irast.Set)
    result = ir_set.expr
    assert isinstance(result, irast.BaseConstant)
    if styperef is not None and result.typeref.id != styperef.id:
        result = type(result)(value=result.value, typeref=styperef)

    return result


def get_param_anchors_for_callable(
    params: s_func.ParameterLikeList,
    schema: s_schema.Schema,
    *,
    inlined_defaults: bool,
) -> Tuple[
    Dict[str, irast.Parameter],
    List[qlast.AliasedExpr],
]:
    anchors = {}
    aliases = []

    if inlined_defaults:
        anchors['__defaults_mask__'] = irast.Parameter(
            name='__defaults_mask__',
            typeref=irtyputils.type_to_typeref(  # note: no cache
                schema,
                cast(s_scalars.ScalarType, schema.get('std::bytes')),
            ),
        )

    pg_params = s_func.PgParams.from_params(schema, params)
    for pi, p in enumerate(pg_params.params):
        p_shortname = p.get_shortname(schema)
        anchors[p_shortname] = irast.Parameter(
            name=p_shortname,
            typeref=irtyputils.type_to_typeref(schema, p.get_type(schema)))

        if p.get_default(schema) is None:
            continue

        if not inlined_defaults:
            continue

        aliases.append(
            qlast.AliasedExpr(
                alias=p_shortname,
                expr=qlast.IfElse(
                    condition=qlast.BinOp(
                        left=qlast.FunctionCall(
                            func=('std', 'bytes_get_bit'),
                            args=[
                                qlast.Path(steps=[
                                    qlast.ObjectRef(
                                        name='__defaults_mask__')
                                ]),
                                qlast.IntegerConstant(value=str(pi)),
                            ]),
                        right=qlast.IntegerConstant(value='0'),
                        op='='),
                    if_expr=qlast.Path(
                        steps=[qlast.ObjectRef(name=p_shortname)]),
                    else_expr=qlast._Optional(expr=p.get_ql_default(schema)))))

    return anchors, aliases
