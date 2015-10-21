##
# Copyright (c) 2015 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import debug

from . import parser
from . import transformer


def compile_fragment_to_ir(expr, schema, *, anchors=None, location=None,
                                            module_aliases=None):
    """Compile given CaosQL expression fragment into Caos IR"""

    tree = parser.parse_fragment(expr)
    trans = transformer.CaosqlTreeTransformer(schema, module_aliases)
    return trans.transform_fragment(tree, (), anchors=anchors,
                                    location=location)


@debug.debug
def compile_to_ir(expr, schema, *, anchors=None, arg_types=None,
                                   security_context=None,
                                   module_aliases=None):
    """Compile given CaosQL statement into Caos IR"""

    """LOG [caosql.compile] CaosQL TEXT:
    print(expr)
    """
    tree = parser.parse(expr, module_aliases)

    """LOG [caosql.compile] CaosQL AST:
    from metamagic.utils import markup
    markup.dump(tree)
    """
    trans = transformer.CaosqlTreeTransformer(schema, module_aliases)

    ir = trans.transform(tree, arg_types, module_aliases=module_aliases,
                         anchors=anchors, security_context=security_context)

    """LOG [caosql.compile] Caos IR:
    from metamagic.utils import markup
    markup.dump(ir)
    """

    return ir
