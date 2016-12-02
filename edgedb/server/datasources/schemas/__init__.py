##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.server.datasources.schemas.base import Base


class Sql(Base):
    def check(self, node):
        cls = 'edgedb.server.datasources.sql.Sql'
        tag = 'tag:importkit.magic.io,2009/importkit/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)


class Yaml(Base):
    def check(self, node):
        cls = 'edgedb.server.datasources.yaml.Yaml'
        tag = 'tag:importkit.magic.io,2009/importkit/class/derive:%s' % cls
        self.push_tag(node, tag)
        return super().check(node)
