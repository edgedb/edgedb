##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql.string

from .. import common
from . import base
from . import ddl


class DomainExists(base.Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        domain_name
                    FROM
                        information_schema.domains
                    WHERE
                        domain_schema = $1 AND domain_name = $2'''
        return code, self.name


class CreateDomain(ddl.SchemaObjectOperation):
    def __init__(self, name, base, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(name, conditions=conditions, neg_conditions=neg_conditions,
                                                      priority=priority)
        self.base = base

    def code(self, context):
        return 'CREATE DOMAIN %s AS %s' % (common.qname(*self.name), self.base)


class RenameDomain(base.CommandGroup):
    def __init__(self, name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        if name[0] != new_name[0]:
            cmd = AlterDomainSetSchema(name, new_name[0])
            self.add_command(cmd)
            name = (new_name[0], name[1])

        if name[1] != new_name[1]:
            cmd = AlterDomainRenameTo(name, new_name[1])
            self.add_command(cmd)


class AlterDomainSetSchema(ddl.DDLOperation):
    def __init__(self, name, new_schema, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_schema = new_schema

    def code(self, context):
        code = 'ALTER DOMAIN {} SET SCHEMA {}'.format(common.qname(*self.name),
                                                      common.quote_ident(self.new_schema))
        return code


class AlterDomainRenameTo(ddl.DDLOperation):
    def __init__(self, name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_name = new_name

    def code(self, context):
        code = 'ALTER DOMAIN {} RENAME TO {}'.format(common.qname(*self.name),
                                                     common.quote_ident(self.new_name))
        return code


class AlterDomain(ddl.DDLOperation):
    def __init__(self, name):
        super().__init__()

        self.name = name


    def code(self, context):
        return 'ALTER DOMAIN %s ' % common.qname(*self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class AlterDomainAlterDefault(AlterDomain):
    def __init__(self, name, default):
        super().__init__(name)
        self.default = default

    def code(self, context):
        code = super().code(context)
        if self.default is None:
            code += ' DROP DEFAULT ';
        else:
            value = postgresql.string.quote_literal(str(self.default)) if self.default is not None else 'None'
            code += ' SET DEFAULT ' + value
        return code


class AlterDomainAlterNull(AlterDomain):
    def __init__(self, name, null):
        super().__init__(name)
        self.null = null

    def code(self, context):
        code = super().code(context)
        if self.null:
            code += ' DROP NOT NULL ';
        else:
            code += ' SET NOT NULL ';
        return code


class AlterDomainAlterConstraint(AlterDomain):
    def __init__(self, name, constraint_name, constraint_code):
        super().__init__(name)
        self._constraint_name = constraint_name
        self._constraint_code = constraint_code


class AlterDomainDropConstraint(AlterDomainAlterConstraint):
    def code(self, context):
        code = super().code(context)
        code += ' DROP CONSTRAINT {} '.format(self._constraint_name)
        return code


class AlterDomainAddConstraint(AlterDomainAlterConstraint):
    def code(self, context):
        code = super().code(context)
        code += ' ADD CONSTRAINT {} {}'.format(self._constraint_name, self._constraint_code)
        return code


class DropDomain(ddl.SchemaObjectOperation):
    def code(self, context):
        return 'DROP DOMAIN %s' % common.qname(*self.name)
