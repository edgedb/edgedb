##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL to IR compiler."""


from edgedb.lang.edgeql import parser
from edgedb.lang.common import debug
from edgedb.lang.common import markup  # NOQA

from . import compiler
from .decompiler import decompile_ir  # NOQA


def compile_fragment_to_ir(expr,
                           schema,
                           *,
                           anchors=None,
                           location=None,
                           modaliases=None):
    """Compile given EdgeQL expression fragment into EdgeDB IR."""
    tree = parser.parse_fragment(expr)
    trans = compiler.EdgeQLCompiler(schema, modaliases)
    return trans.transform_fragment(
        tree, (), anchors=anchors, location=location)


def compile_ast_fragment_to_ir(tree,
                               schema,
                               *,
                               anchors=None,
                               location=None,
                               modaliases=None):
    """Compile given EdgeQL AST fragment into EdgeDB IR."""
    trans = compiler.EdgeQLCompiler(schema, modaliases)
    return trans.transform_fragment(
        tree, (), anchors=anchors, location=location)


@debug.debug
def compile_to_ir(expr,
                  schema,
                  *,
                  anchors=None,
                  arg_types=None,
                  security_context=None,
                  modaliases=None):
    """Compile given EdgeQL statement into EdgeDB IR."""
    """LOG [edgeql.compile] EdgeQL TEXT:
    print(expr)
    """
    tree = parser.parse(expr, modaliases)
    """LOG [edgeql.compile] EdgeQL AST:
    from edgedb.lang.common import markup
    markup.dump(tree)
    """
    trans = compiler.EdgeQLCompiler(schema, modaliases)

    ir = trans.transform(
        tree,
        arg_types,
        modaliases=modaliases,
        anchors=anchors,
        security_context=security_context)
    """LOG [edgeql.compile] EdgeDB IR:
    from edgedb.lang.common import markup
    markup.dump(ir)
    """

    return ir


@debug.debug
def compile_ast_to_ir(tree,
                      schema,
                      *,
                      anchors=None,
                      arg_types=None,
                      security_context=None,
                      modaliases=None):
    """Compile given EdgeQL AST into EdgeDB IR."""
    """LOG [edgeql.compile] EdgeQL AST:
    from edgedb.lang.common import markup
    markup.dump(tree)
    """
    trans = compiler.EdgeQLCompiler(schema, modaliases)

    ir = trans.transform(
        tree,
        arg_types,
        modaliases=modaliases,
        anchors=anchors,
        security_context=security_context)
    """LOG [edgeql.compile] EdgeDB IR:
    from edgedb.lang.common import markup
    markup.dump(ir)
    """

    return ir
