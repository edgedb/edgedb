#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from edgedb.lang.common import ordered

from .. import common
from . import base
from . import composites
from . import ddl


class CompositeType(composites.CompositeDBObject):
    def __init__(self, name, columns=()):
        super().__init__(name)
        self._columns = ordered.OrderedSet(columns)

    def iter_columns(self):
        return iter(self._columns)


class TypeExists(base.Condition):
    def __init__(self, name):
        self.name = name

    async def code(self, context):
        code = '''
            SELECT
                typname
            FROM
                pg_catalog.pg_type typ
                INNER JOIN pg_catalog.pg_namespace nsp
                    ON nsp.oid = typ.typnamespace
            WHERE
                nsp.nspname = $1 AND typ.typname = $2
        '''
        return code, self.name


CompositeTypeExists = TypeExists


class CompositeTypeAttributeExists(base.Condition):
    def __init__(self, type_name, attribute_name):
        self.type_name = type_name
        self.attribute_name = attribute_name

    async def code(self, context):
        code = '''
            SELECT
                attribute_name
            FROM
                information_schema.attributes
            WHERE
                udt_schema = $1 AND udt_name = $2 AND attribute_name = $3'''
        return code, self.type_name + (self.attribute_name, )


class CreateCompositeType(ddl.SchemaObjectOperation):
    def __init__(
            self, type, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            type.name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.type = type

    async def code(self, context):
        elems = [c.code(context, short=True) for c in self.type.iter_columns()]

        name = common.qname(*self.type.name)
        cols = ', '.join(c for c in elems)

        code = 'CREATE TYPE %s AS (%s)' % (name, cols)

        return code


class AlterCompositeTypeBaseMixin:
    def __init__(self, name, **kwargs):
        self.name = name

    def prefix_code(self, context):
        return 'ALTER TYPE {}'.format(common.qname(*self.name))

    def __repr__(self):
        return '<%s.%s %s>' % (
            self.__class__.__module__, self.__class__.__name__, self.name)


class AlterCompositeTypeBase(AlterCompositeTypeBaseMixin, ddl.DDLOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        ddl.DDLOperation.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        AlterCompositeTypeBaseMixin.__init__(self, name=name)


class AlterCompositeTypeFragment(ddl.DDLOperation):
    def get_attribute_term(self):
        return 'ATTRIBUTE'


class AlterCompositeType(
        AlterCompositeTypeBaseMixin, base.CompositeCommandGroup):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        base.CompositeCommandGroup.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        AlterCompositeTypeBaseMixin.__init__(self, name=name)


class AlterCompositeTypeAddAttribute(
        composites.AlterCompositeAddAttribute, AlterCompositeTypeFragment):
    async def code(self, context):
        return 'ADD {} {}'.format(
            self.get_attribute_term(), self.attribute.code(
                context, short=True))


class AlterCompositeTypeDropAttribute(
        composites.AlterCompositeDropAttribute, AlterCompositeTypeFragment):
    pass


class AlterCompositeTypeAlterAttributeType(
        composites.AlterCompositeAlterAttributeType,
        AlterCompositeTypeFragment):
    pass


class AlterCompositeTypeSetSchema(AlterCompositeTypeBase):
    def __init__(self, name, schema, **kwargs):
        super().__init__(name, **kwargs)
        self.schema = schema

    async def code(self, context):
        code = super().prefix_code(context)
        code += ' SET SCHEMA %s ' % common.quote_ident(self.schema)
        return code


class AlterCompositeTypeRenameTo(AlterCompositeTypeBase):
    def __init__(self, name, new_name, **kwargs):
        super().__init__(name, **kwargs)
        self.new_name = new_name

    async def code(self, context):
        code = super().prefix_code(context)
        code += ' RENAME TO %s ' % common.quote_ident(self.new_name)
        return code


class AlterCompositeTypeRenameAttribute(
        composites.AlterCompositeRenameAttribute, AlterCompositeTypeBase):
    def get_attribute_term(self):
        return 'ATTRIBUTE'


class DropCompositeType(ddl.SchemaObjectOperation):
    def __init__(
            self, name, *, cascade=False, conditions=None, neg_conditions=None,
            priority=0):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.cascade = cascade

    async def code(self, context):
        return 'DROP TYPE {}{}'.format(
            common.qname(*self.name), ' CASCADE' if self.cascade else '')
