##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.datasources.schemas.base import Base


class Sql(Base):
    def check(self, node):
        cls = 'semantix.caos.datasources.sql.Sql'
        tag = 'tag:semantix.sprymix.com,2009/semantix/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)


class Python(Base):
    def check(self, node):
        cls = 'semantix.caos.datasources.python.Python'
        tag = 'tag:semantix.sprymix.com,2009/semantix/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)


class Yaml(Base):
    def check(self, node):
        cls = 'semantix.caos.datasources.yaml.Yaml'
        tag = 'tag:semantix.sprymix.com,2009/semantix/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)


class CaosQL(Base):
    def check(self, node):
        cls = 'semantix.caos.datasources.caosql.CaosQL'
        tag = 'tag:semantix.sprymix.com,2009/semantix/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)
