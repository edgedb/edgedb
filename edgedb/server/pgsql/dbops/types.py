##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .. import common
from . import base
from . import composites
from . import ddl


class CompositeType(composites.CompositeDBObject):
    def columns(self):
        return self.__columns


class TypeExists(base.Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        typname
                    FROM
                        pg_catalog.pg_type typ
                        INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = typ.typnamespace
                    WHERE
                        nsp.nspname = $1 AND typ.typname = $2'''
        return code, self.name

CompositeTypeExists = TypeExists


class CompositeTypeAttributeExists(base.Condition):
    def __init__(self, type_name, attribute_name):
        self.type_name = type_name
        self.attribute_name = attribute_name

    def code(self, context):
        code = '''SELECT
                        attribute_name
                    FROM
                        information_schema.attributes
                    WHERE
                        udt_schema = $1 AND udt_name = $2 AND attribute_name = $3'''
        return code, self.type_name + (self.attribute_name,)


class CreateCompositeType(ddl.SchemaObjectOperation):
    def __init__(self, type, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(type.name, conditions=conditions, neg_conditions=neg_conditions,
                         priority=priority)
        self.type = type

    def code(self, context):
        elems = [c.code(context) for c in self.type._columns]

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
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.name)


class AlterCompositeTypeBase(AlterCompositeTypeBaseMixin, ddl.DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        ddl.DDLOperation.__init__(self, conditions=conditions, neg_conditions=neg_conditions,
                                  priority=priority)
        AlterCompositeTypeBaseMixin.__init__(self, name=name)


class AlterCompositeTypeFragment(ddl.DDLOperation):
    def get_attribute_term(self):
        return 'ATTRIBUTE'


class AlterCompositeType(AlterCompositeTypeBaseMixin, base.CompositeCommandGroup):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        base.CompositeCommandGroup.__init__(self, conditions=conditions, neg_conditions=neg_conditions,
                                            priority=priority)
        AlterCompositeTypeBaseMixin.__init__(self, name=name)


class AlterCompositeTypeAddAttribute(composites.AlterCompositeAddAttribute,
                                     AlterCompositeTypeFragment):
    pass


class AlterCompositeTypeDropAttribute(composites.AlterCompositeDropAttribute,
                                      AlterCompositeTypeFragment):
    pass


class AlterCompositeTypeAlterAttributeType(composites.AlterCompositeAlterAttributeType,
                                           AlterCompositeTypeFragment):
    pass


class AlterCompositeTypeSetSchema(AlterCompositeTypeBase):
    def __init__(self, name, schema, **kwargs):
        super().__init__(name, **kwargs)
        self.schema = schema

    def code(self, context):
        code = super().prefix_code(context)
        code += ' SET SCHEMA %s ' % common.quote_ident(self.schema)
        return code


class AlterCompositeTypeRenameTo(AlterCompositeTypeBase):
    def __init__(self, name, new_name, **kwargs):
        super().__init__(name, **kwargs)
        self.new_name = new_name

    def code(self, context):
        code = super().prefix_code(context)
        code += ' RENAME TO %s ' % common.quote_ident(self.new_name)
        return code


class AlterCompositeTypeRenameAttribute(composites.AlterCompositeRenameAttribute,
                                        AlterCompositeTypeBase):
    pass


class DropCompositeType(ddl.SchemaObjectOperation):
    def code(self, context):
        return 'DROP TYPE %s' % common.qname(*self.name)
