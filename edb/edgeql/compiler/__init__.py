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
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    TypeVar,
    AbstractSet,
    Mapping,
    Dict,
    List,
    Set,
    cast,
    overload,
    TYPE_CHECKING,
)

# WARNING: this package is in a tight import loop with various modules
# in edb.schema, so no direct imports from either this package or
# edb.schema are allowed at the top-level.  If absolutely necessary,
# use the lazy-loading mechanism.

import functools

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser

from edb.common import debug

from .options import CompilerOptions as CompilerOptions  # "as" for reexport

if TYPE_CHECKING:
    from edb.schema import schema as s_schema

    from edb.ir import ast as irast
    from edb.ir import staeval as ireval

    from . import dispatch as dispatch_mod
    from . import inference as inference_mod
    from . import normalization as norm_mod
    from . import stmtctx as stmtctx_mod
else:
    # Modules will be loaded lazily in _load().
    dispatch_mod = None
    inference_mod = None
    irast = None
    ireval = None
    norm_mod = None
    stmtctx_mod = None


#: Compiler modules lazy-load guard.
_LOADED = False

Tf = TypeVar('Tf', bound=Callable[..., Any])


def compiler_entrypoint(func: Tf) -> Tf:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not _LOADED:
            _load()
        return func(*args, **kwargs)

    return cast(Tf, wrapper)


@overload
def compile_ast_to_ir(
    tree: qlast.Expr | qlast.Command,
    schema: s_schema.Schema,
    *,
    script_info: Optional[irast.ScriptInfo] = None,
    options: Optional[CompilerOptions] = None,
) -> irast.Statement:
    pass


@overload
def compile_ast_to_ir(
    tree: qlast.ConfigOp,
    schema: s_schema.Schema,
    *,
    script_info: Optional[irast.ScriptInfo] = None,
    options: Optional[CompilerOptions] = None,
) -> irast.ConfigCommand:
    pass


@overload
def compile_ast_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    script_info: Optional[irast.ScriptInfo] = None,
    options: Optional[CompilerOptions] = None,
) -> irast.Statement | irast.ConfigCommand:
    pass


@compiler_entrypoint
def compile_ast_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    script_info: Optional[irast.ScriptInfo] = None,
    options: Optional[CompilerOptions] = None,
) -> irast.Statement | irast.ConfigCommand:
    """Compile given EdgeQL AST into Gel IR.

    This is the normal compiler entry point.  It assumes that *tree*
    represents a complete statement.

    Args:
        tree:
            EdgeQL AST.

        schema:
            Schema instance.  Must contain definitions for objects
            referenced by the AST *tree*.

        options:
            An optional :class:`edgeql.compiler.options.CompilerOptions`
            instance specifying compilation options.

        allow_writing_protected_ptrs:
            If ``True``, allows protected object properties or links to
            be overwritten in `INSERT` shapes.

    Returns:
        An instance of :class:`ir.ast.Command`.  Most frequently, this
        would be an instance of :class:`ir.ast.Statement`.
    """
    if options is None:
        options = CompilerOptions()

    if debug.flags.edgeql_compile or debug.flags.edgeql_compile_edgeql_text:
        debug.header('EdgeQL Text')
        debug.dump_code(qlcodegen.generate_source(tree, pretty=True))

    if debug.flags.edgeql_compile or debug.flags.edgeql_compile_edgeql_ast:
        debug.header('Compiler Options')
        debug.dump(options.__dict__)
        debug.header('EdgeQL AST')
        debug.dump(tree, schema=schema)

    ctx = stmtctx_mod.init_context(schema=schema, options=options)

    if isinstance(tree, qlast.Expr) and ctx.implicit_limit:
        tree = qlast.SelectQuery(result=tree, implicit=True)
        tree.limit = qlast.Constant.integer(ctx.implicit_limit)

    if not script_info:
        script_info = stmtctx_mod.preprocess_script([tree], ctx=ctx)

    ctx.env.script_params = script_info.params

    ir_set = dispatch_mod.compile(tree, ctx=ctx)
    ir_expr = stmtctx_mod.fini_expression(ir_set, ctx=ctx)

    if debug.flags.edgeql_compile or debug.flags.edgeql_compile_scope:
        debug.header('Scope Tree')
        print(ctx.path_scope.pdebugformat())

        # Also build and dump a mapping from scope ids to
        # paths that appear directly at them.
        scopes: Dict[int, Set[irast.PathId]] = {
            k: set() for k in
            sorted(node.unique_id
                   for node in ctx.path_scope.descendants
                   if node.unique_id)
        }
        for ir_set in ctx.env.set_types:
            if ir_set.path_scope_id and ir_set.path_scope_id in scopes:
                scopes[ir_set.path_scope_id].add(ir_set.path_id)
        debug.dump(scopes)

    if debug.flags.edgeql_compile or debug.flags.edgeql_compile_ir:
        debug.header('Gel IR')
        debug.dump(ir_expr, schema=getattr(ir_expr, 'schema', None))

    return ir_expr


@compiler_entrypoint
def compile_ast_fragment_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    options: Optional[CompilerOptions] = None,
) -> irast.Statement:
    """Compile given EdgeQL AST fragment into Gel IR.

    Unlike :func:`~compile_ast_to_ir` above, this does not assume
    that the AST *tree* is a complete statement.  The expression
    doesn't even have to resolve to a specific type.

    Args:
        tree:
            EdgeQL AST fragment.

        schema:
            Schema instance.  Must contain definitions for objects
            referenced by the AST *tree*.

        options:
            An optional :class:`edgeql.compiler.options.CompilerOptions`
            instance specifying compilation options.

    Returns:
        An instance of :class:`ir.ast.Statement`.
    """
    if options is None:
        options = CompilerOptions()

    ctx = stmtctx_mod.init_context(schema=schema, options=options)
    ir_set = dispatch_mod.compile(tree, ctx=ctx)

    result_type = ctx.env.set_types[ir_set]

    return irast.Statement(
        expr=ir_set,
        schema=ctx.env.schema,
        stype=result_type,
        dml_exprs=ctx.env.dml_exprs,
        views={},
        params=[],
        globals=[],
        # These values are nonsensical, but ideally the caller does not care
        cardinality=qltypes.Cardinality.UNKNOWN,
        multiplicity=qltypes.Multiplicity.EMPTY,
        volatility=qltypes.Volatility.Volatile,
        view_shapes={},
        view_shapes_metadata={},
        schema_refs=frozenset(),
        schema_ref_exprs=None,
        scope_tree=ctx.path_scope,
        type_rewrites={},
        singletons=[],
        triggers=(),
        warnings=tuple(ctx.env.warnings),
    )


@compiler_entrypoint
def preprocess_script(
    stmts: List[qlast.Base],
    schema: s_schema.Schema,
    *,
    options: CompilerOptions,
) -> irast.ScriptInfo:
    ctx = stmtctx_mod.init_context(schema=schema, options=options)
    return stmtctx_mod.preprocess_script(stmts, ctx=ctx)


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
    tree = qlparser.parse_fragment(expr)
    return evaluate_ast_to_python_val(tree, schema, modaliases=modaliases)


def evaluate_ir_statement_to_python_val(
    ir: irast.Statement,
) -> Any:
    """Evaluate the given EdgeQL IR AST as a constant expression.

    Args:
        ir:
            EdgeQL IR Statement AST.

    Returns:
        The result of the evaluation as a Python value and the associated IR.

    Raises:
        If the expression is not constant, or is otherwise not supported by
        the const evaluator, the function will raise
        :exc:`ir.staeval.UnsupportedExpressionError`.
    """
    return ireval.evaluate_to_python_val(ir.expr, schema=ir.schema)


def evaluate_ast_to_python_val_and_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
) -> Tuple[Any, irast.Statement]:
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
        The result of the evaluation as a Python value and the associated IR.

    Raises:
        If the expression is not constant, or is otherwise not supported by
        the const evaluator, the function will raise
        :exc:`ir.staeval.UnsupportedExpressionError`.
    """
    if modaliases is None:
        modaliases = {}
    ir = compile_ast_fragment_to_ir(
        tree,
        schema,
        options=CompilerOptions(
            modaliases=modaliases,
        ),
    )
    return ireval.evaluate_to_python_val(ir.expr, schema=ir.schema), ir


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
    return evaluate_ast_to_python_val_and_ir(
        tree, schema, modaliases=modaliases
    )[0]


@compiler_entrypoint
def compile_constant_tree_to_ir(
    const: qlast.BaseConstant,
    schema: s_schema.Schema,
    *,
    styperef: Optional[irast.TypeRef] = None,
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

    Returns:
        An instance of :class:`ir.ast.ConstExpr` representing the
        constant.
    """
    ctx = stmtctx_mod.init_context(schema=schema, options=CompilerOptions())
    if not isinstance(const, qlast.BaseConstant):
        raise ValueError(f'unexpected input: {const!r} is not a constant')

    ir_set = dispatch_mod.compile(const, ctx=ctx)
    assert isinstance(ir_set, irast.Set)
    result = ir_set.expr
    assert isinstance(result, irast.BaseConstant)
    if styperef is not None and result.typeref.id != styperef.id:
        result = type(result)(value=result.value, typeref=styperef)

    return result


@compiler_entrypoint
def normalize(
    tree: qlast.Base,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    """Normalize the given AST *tree* by explicitly qualifying identifiers.

    This helper takes an arbitrary EdgeQL AST tree together with the current
    module alias mapping and produces an equivalent expression, in which
    all identifiers representing schema object references are properly
    qualified with the module name.

    NOTE: the tree is mutated *in-place*.
    """
    return norm_mod.normalize(
        tree,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )


@compiler_entrypoint
def renormalize_compat(
    tree: qlast.Base_T,
    orig_text: str,
    *,
    schema: s_schema.Schema,
    localnames: AbstractSet[str] = frozenset(),
) -> qlast.Base_T:
    """Renormalize an expression normalized with imprint_expr_context().

    This helper takes the original, unmangled expression, an EdgeQL AST
    tree of the same expression mangled with `imprint_expr_context()`
    (which injects extra WITH MODULE clauses), and produces a normalized
    expression with explicitly qualified identifiers instead.  Old dumps
    are the main user of this facility.
    """
    return norm_mod.renormalize_compat(
        tree,
        orig_text,
        schema=schema,
        localnames=localnames,
    )


def _load() -> None:
    """Load the compiler modules.  This is done once per process."""

    global _LOADED
    global dispatch_mod, inference_mod, irast, ireval, norm_mod, stmtctx_mod

    from edb.ir import ast as _irast
    from edb.ir import staeval as _ireval

    from . import expr as _expr_compiler  # NOQA
    from . import config as _config_compiler  # NOQA
    from . import stmt as _stmt_compiler  # NOQA

    from . import dispatch
    from . import inference
    from . import normalization
    from . import stmtctx

    dispatch_mod = dispatch
    inference_mod = inference
    irast = _irast
    ireval = _ireval
    norm_mod = normalization
    stmtctx_mod = stmtctx
    _LOADED = True
