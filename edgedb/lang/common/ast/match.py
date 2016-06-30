##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Generic AST tree pattern matching."""


import collections

from edgedb.lang.common import ast
from edgedb.lang.common.functional import adapter


class MatchASTMeta(adapter.Adapter, ast.MetaAST):
    pass


class MatchASTNode:
    def __init__(self, **kwargs):
        self.fields = kwargs

    def __setattr__(self, name, value):
        if name in self._fields:
            self.fields[name] = value
        object.__setattr__(self, name, value)

    def __iter__(self):
        for field_name, field_value in self.fields.items():
            yield field_name, field_value


class Match:
    def __init__(self):
        pass


class MatchGroup:
    def __init__(self):
        pass

    def add(self, group_name, node):
        nodes = getattr(self, group_name, None)

        if nodes is None:
            nodes = []
            setattr(self, group_name, nodes)
        nodes.append(node)


class MatchNode:
    pass


class MatchGroupNode(MatchNode):
    def __init__(self, name, node):
        self.name = name
        self.node = node


class AlternativeMatchPattern(MatchNode):
    def __init__(self, alternatives):
        self.alternatives = alternatives


class MatchContextWrapper:
    def __init__(self, context):
        self.context = context

    def __enter__(self):
        self.context.push()
        return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class MatchContext:
    def __init__(self):
        self.stack = []
        self.stack.append(MatchGroup())

    def push(self):
        new_group = MatchGroup()
        self.stack.append(new_group)
        return new_group

    def pop(self):
        return self.stack.pop()

    def get_match_group(self):
        return self.stack[-1]

    def get_match(self):
        pass

    def __call__(self):
        return MatchContextWrapper(self)


def Or(*exprs):
    return AlternativeMatchPattern(exprs)


def group(name, node):
    return MatchGroupNode(name, node)


def _match_node(pattern, node, context):
    if not issubclass(node.__class__, pattern.__class__.get_adaptee()):
        return None

    for field_name, field_value in pattern:
        node_value = getattr(node, field_name)

        if isinstance(field_value, MatchNode):
            m = _match(field_value, node_value, context)

            if not m:
                return None

        else:
            if not issubclass(field_value.__class__, node_value.__class__):
                return None

            if isinstance(field_value, collections.Container) and not isinstance(field_value, str):
                if len(field_value) != len(node_value):
                    return None

                for cfv, cnv in zip(field_value, node_value):
                    if not _match(cfv, cnv, context):
                        return None

            elif isinstance(field_value, MatchASTNode):
                m = _match_node(field_value, node_value, context)

                if not m:
                    return None

            else:
                if not field_value == node_value:
                    return None

    return True


def _match(pattern, node, context):
    result = None

    if isinstance(pattern, AlternativeMatchPattern):
        for alternative in pattern.alternatives:
            result = _match(alternative, node, context)
            if result:
                break

    elif isinstance(pattern, MatchGroupNode):
        with context():
            result = _match(pattern.node, node, context)
            if result:
                result = context.get_match_group()
                result.node = node

        if result:
            match_group = context.get_match_group()
            match_group.add(pattern.name, result)

    else:
        result = _match_node(pattern, node, context)

    return result


def match(pattern, node):
    context = MatchContext()
    result = _match(pattern, node, context)
    if result:
        return context.get_match_group()
    else:
        return None
