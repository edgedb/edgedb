##
# Copyright (c) 2010-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from metamagic import caos
from metamagic.caos import proto
from metamagic.utils import ast
from . import ast as caosql_ast, parser, transformer, codegen
from metamagic.caos.tree import ast as caos_ast
from metamagic.caos.tree import transformer as caos_transformer
from metamagic.caos import caosql

from metamagic.utils import debug

from . import errors


class CaosQLExpression:
    def __init__(self, proto_schema, module_aliases=None):
        self.parser = parser.CaosQLParser()
        self.module_aliases = module_aliases
        self.proto_schema = proto_schema
        self.transformer = transformer.CaosqlTreeTransformer(proto_schema, module_aliases)
        self.reverse_transformer = transformer.CaosqlReverseTransformer()

    def get_statement_tree(self, expr):
        tree = self.parser.parse(expr)

        if not isinstance(tree, caosql_ast.StatementNode):
            selnode = caosql_ast.SelectQueryNode()
            selnode.targets = [caosql_ast.SelectExprNode(expr=tree)]
            tree = selnode

        if self.module_aliases:
            nses = []
            for alias, module in self.module_aliases.items():
                decl = caosql_ast.NamespaceDeclarationNode(namespace=module,
                                                           alias=alias)
                nses.append(decl)

            if tree.namespaces is None:
                tree.namespaces = nses
            else:
                tree.namespaces.extend(nses)

        return tree

    def normalize_tree(self, expr, module_aliases=None, anchors=None,
                                   inline_anchors=False):
        caosql_tree = self.get_statement_tree(expr)
        caos_tree = self.transformer.transform(
                        caosql_tree, (), module_aliases=module_aliases,
                        anchors=anchors)
        caosql_tree = self.reverse_transformer.transform(
                        caos_tree, inline_anchors=inline_anchors)

        source = codegen.CaosQLSourceGenerator.to_source(
                        caosql_tree, pretty=False)

        return caos_tree, caosql_tree, source

    def normalize_expr(self, expr, module_aliases=None, anchors=None,
                                   inline_anchors=False):
        _, _, source = self.normalize_tree(
            expr, module_aliases=module_aliases, anchors=anchors,
            inline_anchors=inline_anchors)

        return source

    def transform_expr_fragment(self, expr, anchors=None, location=None):
        tree = self.parser.parse(expr)
        return self.transformer.transform_fragment(
                        tree, (), anchors=anchors, location=location)

    @debug.debug
    def transform_expr(self, expr, anchors=None, arg_types=None,
                                   security_context=None):
        """LOG [caos.query] CaosQL query:
        print(expr)
        """

        caosql_tree = self.get_statement_tree(expr)

        """LOG [caos.query] CaosQL tree:
        from metamagic.utils import markup
        markup.dump(caosql_tree)
        """

        query_tree = self.transformer.transform(
                        caosql_tree, arg_types,
                        module_aliases=self.module_aliases,
                        anchors=anchors, security_context=security_context)

        """LOG [caos.query] Caos tree:
        from metamagic.utils import markup
        markup.dump(query_tree)
        """

        return query_tree

    def get_source_references(self, tree):
        result = []

        refs = self.transformer.extract_paths(tree, reverse=True, resolve_arefs=True,
                                                    recurse_subqueries=-1)

        if refs is not None:
            flt = lambda n: isinstance(n, (caos_ast.EntitySet, caos_ast.EntityLink))
            nodes = ast.find_children(refs, flt)
            if nodes:
                for node in nodes:
                    if isinstance(node, caos_ast.EntitySet):
                        result.append(node.concept)
                    else:
                        result.append(node.link_proto)

        return set(result)

    def get_terminal_references(self, tree):
        result = set()

        refs = self.transformer.extract_paths(tree, reverse=True, resolve_arefs=True,
                                                    recurse_subqueries=1)

        if refs is not None:
            flt = lambda n: callable(getattr(n, 'is_terminal', None)) and n.is_terminal()
            result.update(ast.find_children(refs, flt))

        return result

    def infer_arg_types(self, tree, protoschema):
        def flt(n):
            if isinstance(n, caos_ast.BinOp):
                return (isinstance(n.left, caos_ast.Constant) or
                        isinstance(n.right, caos_ast.Constant))

        ops = ast.find_children(tree, flt)

        arg_types = {}

        for binop in ops:
            typ = None

            if isinstance(binop.right, caos_ast.Constant):
                expr = binop.left
                arg = binop.right
                reversed = False
            else:
                expr = binop.right
                arg = binop.left
                reversed = True

            if arg.index is None:
                continue

            if isinstance(binop.op, caos_ast.CaosMatchOperator):
                typ = protoschema.get('metamagic.caos.builtins.str')

            elif isinstance(binop.op, (ast.ops.ComparisonOperator, ast.ops.ArithmeticOperator)):
                typ = self.transformer.get_expr_type(expr, protoschema)

            elif isinstance(binop.op, ast.ops.MembershipOperator) and not reversed:
                elem_type = self.transformer.get_expr_type(expr, protoschema)
                typ = proto.Set(element_type=elem_type)

            elif isinstance(binop.op, ast.ops.BooleanOperator):
                typ = protoschema.get('metamagic.caos.builtins.bool')

            else:
                msg = 'cannot infer expr type: unsupported operator: {!r}'.format(binop.op)
                raise ValueError(msg)

            if typ is None:
                msg = 'cannot infer expr type'
                raise ValueError(msg)

            try:
                existing = arg_types[arg.index]
            except KeyError:
                arg_types[arg.index] = typ
            else:
                if existing != typ:
                    msg = 'cannot infer expr type: ambiguous resolution: {!r} and {!r}'
                    raise ValueError(msg.format(existing, typ))

        return arg_types

    def inline_constants(self, caosql_tree, values, types):
        flt = lambda n: isinstance(n, caosql_ast.ConstantNode) and n.index in values
        constants = ast.find_children(caosql_tree, flt)

        for constant in constants:
            value = values[constant.index]

            if isinstance(value, collections.Container) and not isinstance(value, (str, bytes)):
                elements = [caosql_ast.ConstantNode(value=i) for i in value]
                value = caosql_ast.SequenceNode(elements=elements)

            constant.value = value
