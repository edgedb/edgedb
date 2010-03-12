##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.datasources.schemas.base import Base

class Sql(Base):
    def check(self, node):
        if not hasattr(node, 'tags'):
            node.tags = [node.tag]
        else:
            node.tags.add(node.tag)
        node.tag = 'tag:semantix.sprymix.com,2009/semantix/class/derive:semantix.caos.datasources.sql.Sql'
        return super().check(node)


class Python(Base):
    def check(self, node):
        if not hasattr(node, 'tags'):
            node.tags = [node.tag]
        else:
            node.tags.add(node.tag)
        node.tag = 'tag:semantix.sprymix.com,2009/semantix/class/derive:semantix.caos.datasources.python.Python'
        return super().check(node)


class Yaml(Base):
    def check(self, node):
        if not hasattr(node, 'tags'):
            node.tags = [node.tag]
        else:
            node.tags.add(node.tag)
        node.tag = 'tag:semantix.sprymix.com,2009/semantix/class/derive:semantix.caos.datasources.yaml.Yaml'
        return super().check(node)
