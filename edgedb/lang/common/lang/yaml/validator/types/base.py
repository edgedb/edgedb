##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml


class SchemaType(object):
    __slots__ = ['schema', 'constraints', 'dct', 'resolver']

    def __init__(self, schema):
        self.schema = schema
        self.constraints = {}
        self.resolver = yaml.resolver.Resolver()

    def _init_constrainrs(self, constraints, dct):
        for const in constraints:
            if const in dct:
                self.constraints[const] = dct[const]

    def load(self, dct):
        self.dct = dct

    def begin_checks(self):
        pass

    def end_checks(self):
        pass

    def check(self, node):
        if 'object' in self.dct:
            tag = 'tag:semantix.sprymix.com,2009/semantix/object/create:' + self.dct['object']
            self.push_tag(node, tag)
        return node

    def is_bool(self, value):
        return (isinstance(value, str) and str == 'true' or str == 'yes') or bool(value)

    def coerse_value(self, type, value, node):
        if value is None:
            value = yaml.nodes.ScalarNode(value=None, tag='tag:yaml.org,2002:null')
        else:
            node_type = type.default_node_type()
            tag = self.resolver.resolve(node_type, repr(value), (True, False))

            if issubclass(node_type, yaml.nodes.ScalarNode):
                value = str(value)
            value = node_type(value=value, tag=tag, start_mark=node.start_mark,
                              end_mark=node.end_mark)
            value = type.check(value)

        return value

    def default_node_type(self):
        return yaml.nodes.ScalarNode

    def check_tag(self, node, tag, allow_null=True):
        return node.tag == tag or hasattr(node, 'tags') and tag in node.tags \
                               or allow_null and node.tag == 'tag:yaml.org,2002:null'

    def push_tag(self, node, tag):
        if not hasattr(node, 'tags'):
            node.tags = [node.tag]
        else:
            node.tags.add(node.tag)
        node.tag = tag

        return tag
