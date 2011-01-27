##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import copy
import yaml

from .composite import CompositeType
from ..error import SchemaValidationError

class MappingType(CompositeType):
    __slots__ = ['keys', 'unique_base', 'unique', 'ordered']

    def __init__(self, schema):
        super().__init__(schema)
        self.keys = collections.OrderedDict()

        self.unique_base = {}
        self.unique = None

    def default_node_type(self):
        return yaml.nodes.MappingNode

    def load_keys(self, keys):
        for key, value in keys.items():
            self.keys[key] = {}

            if isinstance(value, dict):
                self.keys[key]['required'] = 'required' in value and value['required']
                self.keys[key]['unique'] = 'unique' in value and value['unique']

                if self.keys[key]['unique']:
                    self.unique_base[key] = {}

                if 'default' in value:
                    self.keys[key]['default'] = value['default']
            else:
                self.keys[key]['required'] = False
                self.keys[key]['unique'] = False

            self.keys[key]['type'] = self.schema._build(value)

    def load(self, dct):
        super(MappingType, self).load(dct)

        self._init_constrainrs(('max-length', 'min-length'), dct)
        self.load_keys(dct['mapping'])
        self.ordered = dct.get('ordered', False)

    def begin_checks(self):
        super(MappingType, self).begin_checks()
        self.unique = copy.deepcopy(self.unique_base)

    def end_checks(self):
        super(MappingType, self).end_checks()
        self.unique = None

    def check(self, node):
        node = super().check(node)

        """ XXX:
        did = id(data)
        if did in self.checked:
            return data
        self.checked[did] = True
        """

        if node.tag == 'tag:yaml.org,2002:null':
            node = yaml.nodes.MappingNode(tag='tag:yaml.org,2002:map', value=[],
                                          start_mark=node.start_mark, end_mark=node.end_mark)
        elif not isinstance(node, yaml.nodes.MappingNode):
            raise SchemaValidationError('mapping expected', node)

        if 'min-length' in self.constraints:
            if len(node.value) < self.constraints['min-length']:
                raise SchemaValidationError('the number of elements in mapping must not be less than %d'
                                            % self.constraints['min-length'], node)

        if 'max-length' in self.constraints:
            if len(node.value) > self.constraints['max-length']:
                raise SchemaValidationError('the number of elements in mapping must not exceed %d'
                                            % self.constraints['max-length'], node)

        any = '=' in self.keys

        keys = set()

        for i, (key, value) in enumerate(node.value):
            if isinstance(key.value, list):
                key.tag = 'tag:yaml.org,2002:python/tuple'
                key.value = tuple(key.value)

            if key.value in keys:
                raise SchemaValidationError('duplicate mapping key "%s"' % key.value, node)

            conf_key = key.value
            if key.value in self.keys:
                conf = self.keys[key.value]
            elif any:
                conf_key = '='
                conf = self.keys['=']
            else:
                raise SchemaValidationError('unexpected key "%s"' % key.value, node)

            if conf['required'] and value.value is None:
                raise SchemaValidationError('None value for required key "%s"' % key, node)

            conf['type'].begin_checks()
            value = conf['type'].check(value)
            conf['type'].end_checks()

            if conf['unique']:
                if value.value in self.unique[conf_key]:
                    raise SchemaValidationError('unique key "%s", value "%s" is already used in %s' %
                                                (key.value, value.value, self.unique[conf_key][value.value]))

                self.unique[conf_key][value.value] = value

            node.value[i] = (key, value)
            keys.add(key.value)

        value = {key.value: (key, value) for key, value in node.value}

        for key, conf in self.keys.items():
            if key == '=':
                continue

            if key not in value:
                if 'default' in conf:
                    key = yaml.nodes.ScalarNode(value=key, tag='tag:yaml.org,2002:str')
                    default = self.coerse_value(conf['type'], conf['default'], node)
                    node.value.append((key, default))

                else:
                    if conf['required']:
                        raise SchemaValidationError('key "%s" is required' % key, node)
                    else:
                        k = yaml.nodes.ScalarNode(value=key, tag='tag:yaml.org,2002:str')
                        v = yaml.nodes.ScalarNode(value=None, tag='tag:yaml.org,2002:null')
                        node.value.append((k, v))

        if self.ordered:
            self.push_tag(node, 'tag:semantix.sprymix.com,2009/semantix/orderedmap')

        return node
