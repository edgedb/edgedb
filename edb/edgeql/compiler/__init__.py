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

# WARNING: this package is in a tight import loop with various modules
# in edb.schema, so no direct imports from either this package or
# edb.schema are allowed at the top-level.  If absolutely necessary,
# use the lazy-loading mechanism.

import functools

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import parser as qlparser

from edb.common import debug

from .options import CompilerOptions as CompilerOptions  # "as" for reexport

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
    from edb.schema import types as s_types

    from edb.ir import ast as irast
    from edb.ir import staeval as ireval

    from . import dispatch as dispatch_mod
    from . import inference as inference_mod
    from . import stmtctx as stmtctx_mod
else:
    # Modules will be loaded lazily in _load().
    dispatch_mod = None
    inference_mod = None
    irast = None
    ireval = None
    stmtctx_mod = None


#: Compiler modules lazy-load guard.
_LOADED = False


def compiler_entrypoint(func: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not _LOADED:
            _load()
        return func(*args, **kwargs)

    return wrapper


@compiler_entrypoint
def compile_ast_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    options: Optional[CompilerOptions] = None,
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

        options:
            An optional :class:`edgeql.compiler.options.CompilerOptions`
            instance specifying compilation options.

    Returns:
        An instance of :class:`ir.ast.Command`.  Most frequently, this
        would be an instance of :class:`ir.ast.Statement`.
    """
    if options is None:
        options = CompilerOptions()

    if debug.flags.edgeql_compile:
        debug.header('EdgeQL AST')
        debug.dump(tree, schema=schema)
        debug.header('Compiler Options')
        debug.dump(options.__dict__)

    ctx = stmtctx_mod.init_context(schema=schema, options=options)

    ir_set = dispatch_mod.compile(tree, ctx=ctx)
    ir_expr = stmtctx_mod.fini_expression(ir_set, ctx=ctx)

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


@compiler_entrypoint
def compile_ast_fragment_to_ir(
    tree: qlast.Base,
    schema: s_schema.Schema,
    *,
    options: Optional[CompilerOptions] = None,
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

    result_type: Optional[s_types.Type]
    try:
        result_type = inference_mod.infer_type(ir_set, ctx.env)
    except errors.QueryError:
        # Not all fragments can be resolved into a concrete type,
        # that's OK.
        result_type = None

    return irast.Statement(
        expr=ir_set,
        schema=ctx.env.schema,
        stype=result_type,
    )


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
    if modaliases is None:
        modaliases = {}
    ir = compile_ast_fragment_to_ir(
        tree,
        schema,
        options=CompilerOptions(
            modaliases=modaliases,
        ),
    )
    return ireval.evaluate_to_python_val(ir.expr, schema=ir.schema)


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


def _load() -> None:
    """Load the compiler modules.  This is done once per process."""

    global _LOADED
    global dispatch_mod, inference_mod, irast, ireval, stmtctx_mod

    from edb.ir import ast as _irast
    from edb.ir import staeval as _ireval

    from . import expr as _expr_compiler  # NOQA
    from . import config as _config_compiler  # NOQA
    from . import stmt as _stmt_compiler  # NOQA

    from . import dispatch
    from . import inference
    from . import stmtctx

    dispatch_mod = dispatch
    inference_mod = inference
    irast = _irast
    ireval = _ireval
    stmtctx_mod = stmtctx
    _LOADED = True
