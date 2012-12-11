##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.datasources.schemas.base import Base


class Sql(Base):
    def check(self, node):
        cls = 'metamagic.caos.datasources.sql.Sql'
        tag = 'tag:metamagic.sprymix.com,2009/metamagic/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)


class Python(Base):
    def check(self, node):
        cls = 'metamagic.caos.datasources.python.Python'
        tag = 'tag:metamagic.sprymix.com,2009/metamagic/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)


class Yaml(Base):
    def check(self, node):
        cls = 'metamagic.caos.datasources.yaml.Yaml'
        tag = 'tag:metamagic.sprymix.com,2009/metamagic/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)


class CaosQL(Base):
    def check(self, node):
        cls = 'metamagic.caos.datasources.caosql.CaosQL'
        tag = 'tag:metamagic.sprymix.com,2009/metamagic/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)
