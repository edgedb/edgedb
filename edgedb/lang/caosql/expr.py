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
        self.path_resolver = None

    def parse_expr(self, expr):
        return self.parser.parse(expr)

    def get_statement_tree(self, expr):
        tree = self.parse_expr(expr)

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

    def process_concept_expr(self, expr, concept):
        tree = self.parser.parse(expr)
        context = transformer.ParseContext()
        context.current.location = 'selector'
        return self.transformer._process_expr(context, tree)

    def normalize_refs(self, expr, module_aliases=None):
        tree = self.parser.parse(expr)
        tree = self.transformer.normalize_refs(tree, module_aliases=module_aliases)
        return codegen.CaosQLSourceGenerator.to_source(tree, pretty=False)

    def normalize_tree(self, tree, module_aliases=None, anchors=None, inline_anchors=False):
        if not isinstance(tree, caosql_ast.StatementNode):
            selnode = caosql_ast.SelectQueryNode()
            selnode.targets = [caosql_ast.SelectExprNode(expr=tree)]
            tree = selnode

        caos_tree = self.transformer.transform(tree, (), module_aliases=module_aliases,
                                                         anchors=anchors)
        caosql_tree = self.reverse_transformer.transform(caos_tree, inline_anchors=inline_anchors)
        return caosql_tree, caos_tree

    def normalize_expr(self, expr, module_aliases=None, anchors=None):
        tree = self.parser.parse(expr)
        tree, caos_tree = self.normalize_tree(tree, module_aliases=module_aliases, anchors=anchors)
        return codegen.CaosQLSourceGenerator.to_source(tree, pretty=False), caos_tree

    def transform_expr_fragment(self, expr, anchors=None, location=None):
        tree = self.parser.parse(expr)
        return self.transformer.transform_fragment(tree, (), anchors=anchors, location=location)

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

    def get_path_targets(self, expr, session, anchors=None):
        if not self.path_resolver:
            self.path_resolver = caos_transformer.PathResolver()
        ctree = self.transform_expr_fragment(expr, anchors=anchors)
        targets = self.path_resolver.resolve_path(ctree, session)
        return targets

    def normalize_source_expr(self, expr, source):
        tree = self.parser.parse(expr)

        visitor = _PrependSource(source, self.proto_schema, self.module_aliases)
        visitor.visit(tree)

        expr = codegen.CaosQLSourceGenerator.to_source(tree, pretty=False)
        return expr, tree

    def check_source_atomic_expr(self, tree, source):
        context = transformer.ParseContext()
        context.current.location = 'selector'
        processed = self.transformer._process_expr(context, tree)

        ok = isinstance(processed, caos_ast.BaseRef) \
             or (isinstance(processed, caos_ast.Disjunction) and
                 isinstance(list(processed.paths)[0], caos_ast.BaseRef))

        if not ok:
            msg = "invalid link reference"
            details = "Expression must only contain references to local atoms"
            raise errors.CaosQLReferenceError(msg, details=details)

        return processed

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


class _PrependSource(ast.visitor.NodeVisitor):
    def __init__(self, source, schema, module_aliases):
        self.source = source
        self.schema = schema
        self.module_aliases = module_aliases

    def visit_PathNode(self, node):
        step = node.steps[0]

        if step.namespace:
            name = caos.Name(name=step.expr, module=step.namespace)
        else:
            name = step.expr

        if isinstance(self.source, caos.types.ProtoLink):
            type = proto.LinkProperty
            strtype = 'property'
        else:
            type = proto.Link
            strtype = 'link'

        prototype = self.schema.get(name, None)

        if not prototype:
            prototype = self.schema.get(name, type=type, module_aliases=self.module_aliases)

        if not isinstance(prototype, self.source.__class__.get_canonical_class()):

            pointer_node = caosql_ast.LinkNode(name=prototype.name.name,
                                               namespace=prototype.name.module,
                                               type=strtype)

            if isinstance(self.source, caos.types.ProtoLink):
                link = caosql_ast.LinkPropExprNode(expr=pointer_node)
            else:
                link = caosql_ast.LinkExprNode(expr=pointer_node)

            source = self.source.get_pointer_origin(prototype.name, farthest=True)
            source = caosql_ast.PathStepNode(expr=source.name.name, namespace=source.name.module)
            node.steps[0] = source
            node.steps.insert(1, link)
            offset = 2
        else:
            offset = 0

        steps = []
        for step in node.steps[offset:]:
            steps.append(self.visit(step))
        node.steps[offset:] = steps
        return node

    def visit_PathStepNode(self, node):
        if node.namespace:
            name = caos.Name(name=node.expr, module=node.namespace)
        else:
            name = node.expr

        if isinstance(self.source, caos.types.ProtoLink):
            type = proto.LinkProperty
        else:
            type = proto.Link

        prototype = self.schema.get(name, type=type, module_aliases=self.module_aliases)

        node.expr = prototype.name.name
        node.namespace = prototype.name.module
        return node

    def visit_LinkExprNode(self, node):
        expr = self.visit(node.expr)

        if isinstance(self.source, caos.types.ProtoLink):
            node = caosql_ast.LinkPropExprNode(expr=expr)

        return node

    def visit_LinkNode(self, node):
        if node.namespace:
            name = caos.Name(name=node.name, module=node.namespace)
        else:
            name = node.expr

        prototype = self.schema.get(name, module_aliases=self.module_aliases)

        node.name = prototype.name.name
        node.namespace = prototype.name.module
        return node
