##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml

class SchemaType(object):
    __slots__ = ['schema', 'constraints', 'dct']

    def __init__(self, schema):
        self.schema = schema
        self.constraints = {}

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
            if not hasattr(node, 'tags'):
                node.tags = []
            node.tags.append(node.tag)
            node.tag = 'tag:semantix.sprymix.com,2009/semantix/object/create:' + self.dct['object']
        return node

    def is_bool(self, value):
        return (isinstance(value, str) and str == 'true' or str == 'yes') or bool(value)

    def coerse_value(self, type, value, node):
        # XXX: put proper check here
        if value == 'None' or value is None:
            return yaml.nodes.ScalarNode(value=None, tag='tag:yaml.org,2002:null')
        else:
            return yaml.nodes.ScalarNode(value=value, tag='tag:yaml.org,2002:str')

    def check_tag(self, node, tag):
        return node.tag == tag or hasattr(node, 'tags') and tag in node.tags
