##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections.abc
import itertools
import json
import pickle
import re
import uuid

from edgedb.lang.edgeql import ast as ql_ast
from edgedb.lang.edgeql import compiler as ql_compiler
from edgedb.lang.edgeql import errors as ql_errors

from edgedb.lang.schema import attributes as s_attrs
from edgedb.lang.schema import scalars as s_scalars
from edgedb.lang.schema import objtypes as s_objtypes
from edgedb.lang.schema import constraints as s_constr
from edgedb.lang.schema import database as s_db
from edgedb.lang.schema import delta as sd
from edgedb.lang.schema import error as s_err
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import functions as s_funcs
from edgedb.lang.schema import indexes as s_indexes
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_props
from edgedb.lang.schema import modules as s_mod
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import named as s_named
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import policy as s_policy
from edgedb.lang.schema import referencing as s_referencing
from edgedb.lang.schema import sources as s_sources
from edgedb.lang.schema import types as s_types

from edgedb.lang.common import ordered
from edgedb.lang.common import debug
from edgedb.lang.common import markup, nlang

from edgedb.lang.ir import utils as irutils

from edgedb.server.pgsql import common
from edgedb.server.pgsql import dbops, deltadbops, metaschema

from . import ast as pg_ast
from . import compiler
from . import codegen
from . import datasources
from . import schemamech
from . import types


BACKEND_FORMAT_VERSION = 30
TYPE_ID_NAMESPACE = uuid.UUID('00e50276-2502-11e7-97f2-27fe51238dbd')


class CommandMeta(sd.CommandMeta):
    pass


class ObjectCommandMeta(sd.ObjectCommandMeta, CommandMeta):
    _transparent_adapter_subclass = True


class ReferencedObjectCommandMeta(
        s_referencing.ReferencedObjectCommandMeta, ObjectCommandMeta):
    _transparent_adapter_subclass = True


class MetaCommand(sd.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = ordered.OrderedSet()

    def apply(self, schema, context=None):
        for op in self.ops:
            self.pgops.add(op)

    async def execute(self, context):
        if debug.flags.delta_execute:
            debug.print('EXECUTING', repr(self))

        for op in sorted(
                self.pgops, key=lambda i: getattr(i, 'priority', 0),
                reverse=True):
            await op.execute(context)

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.pgops:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node


class CommandGroupAdapted(MetaCommand, adapts=sd.CommandGroup):
    def apply(self, schema, context):
        sd.CommandGroup.apply(self, schema, context)
        MetaCommand.apply(self, schema, context)


class ObjectMetaCommand(MetaCommand, sd.ObjectCommand,
                        metaclass=ObjectCommandMeta):
    pass


class Record:
    def __init__(self, items):
        self._items = items

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        return '<_Record {!r}>'.format(self._items)


_TypeDesc = collections.namedtuple(
    '_TypeDesc', ['id', 'maintype', 'name', 'collection',
                  'subtypes', 'dimensions', 'is_root'],
    module=__name__)


class TypeDesc(_TypeDesc):
    def __new__(cls, **kwargs):
        if not kwargs.get('id'):
            kwargs['id'] = cls._get_id(kwargs)
        return super().__new__(cls, **kwargs)

    @classmethod
    def _get_id(cls, data):
        s = (
            f"{data['maintype']!r}\x00{data['name']!r}\x00"
            f"{data['collection']!r}\x00"
            f"{','.join(str(s) for s in data['subtypes'])}\x00"
            f"{':'.join(str(d) for d in data['dimensions'])}"
        )

        return uuid.uuid5(TYPE_ID_NAMESPACE, s)


class NamedObjectMetaCommand(
        ObjectMetaCommand, s_named.NamedObjectCommand):
    op_priority = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._type_mech = schemamech.TypeMech()

    def _get_name(self, value):
        if isinstance(value, s_obj.ObjectRef):
            name = value.classname
        elif isinstance(value, s_named.NamedObject):
            name = value.name
        else:
            raise ValueError(
                'expecting a ObjectRef or a '
                'NamedObject, got {!r}'.format(value))

        return name

    def _get_typedesc(self, types, typedesc, is_root=True):
        result = []
        indexes = []
        for tn, t in types:
            # Fill the result with placeholders as we want the
            # parent types to go first.
            typedesc.append(())
            indexes.append(len(typedesc) - 1)

        for i, (tn, t) in enumerate(types):
            if isinstance(t, s_types.Collection):
                if isinstance(t, s_types.Tuple) and t.named:
                    stypes = t.element_types.items()
                else:
                    stypes = [(None, st) for st in t.get_subtypes()]

                subtypes = self._get_typedesc(stypes, typedesc, is_root=False)
                if isinstance(t, s_types.Array):
                    dimensions = t.dimensions
                else:
                    dimensions = []
                desc = TypeDesc(
                    maintype=None, name=tn, collection=t.schema_name,
                    subtypes=subtypes, dimensions=dimensions, is_root=is_root)
            else:
                desc = TypeDesc(
                    maintype=self._get_name(t), name=tn, collection=None,
                    subtypes=[], dimensions=[], is_root=is_root)

            typedesc[indexes[i]] = desc
            result.append(desc.id)

        return result

    def _serialize_field(self, value, col):
        recvalue = None

        if isinstance(value, (s_obj.ObjectSet, s_obj.ObjectList)):
            result = tuple(self._get_name(v) for v in value)

        elif isinstance(value, s_obj.ObjectDict):
            result = []
            self._get_typedesc(value.items(), result)

        elif isinstance(value, s_obj.ObjectCollection):
            result = []
            self._get_typedesc([(None, v) for v in value], result)

        elif isinstance(value, s_obj.Object):
            result = []
            self._get_typedesc([(None, value)], result)

        elif isinstance(value, sn.SchemaName):
            result = value
            recvalue = str(value)

        elif isinstance(value, nlang.WordCombination):
            result = value
            recvalue = json.dumps(value.as_dict())

        elif isinstance(value, collections.abc.Mapping):
            # Other dicts are JSON'ed by default
            result = value
            recvalue = json.dumps(dict(value))

        else:
            result = value

        if result is not value and recvalue is None:
            names = result
            if isinstance(names, list):
                recvalue = dbops.Query(
                    '''SELECT edgedb._encode_type(
                        ROW($1::edgedb.type_desc_node_t[])::edgedb.typedesc_t)
                    ''',
                    [names], type='edgedb.type_t')
            else:
                recvalue = dbops.Query(
                    '''SELECT array_agg(id) FROM edgedb.NamedObject
                       WHERE name = any($1::text[])''',
                    [names], type='uuid[]')

        elif recvalue is None:
            recvalue = result

        return result, recvalue

    def get_fields(self, schema):
        if isinstance(self, sd.CreateObject):
            fields = self.scls
        else:
            fields = self.get_struct_properties(schema)

        return fields

    def fill_record(self, schema):
        updates = {}

        rec = None
        table = self.table

        fields = self.get_fields(schema)

        for name, value in fields.items():
            col = table.get_column(name)

            v1, refqry = self._serialize_field(value, col)

            updates[name] = v1
            if col is not None:
                if rec is None:
                    rec = table.record()
                setattr(rec, name, refqry)

        return rec, updates

    def pack_default(self, value):
        if value is not None:
            if isinstance(value, s_expr.ExpressionText):
                valtype = 'expr'
            else:
                valtype = 'literal'
            val = {'type': valtype, 'value': value}
            result = json.dumps(val)
        else:
            result = None
        return result

    def create_object(self, schema, scls):
        rec, updates = self.fill_record(schema)
        self.pgops.add(
            dbops.Insert(
                table=self.table, records=[rec], priority=self.op_priority))
        return updates

    def update(self, schema, context):
        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(self.scls.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition,
                    priority=self.op_priority))

        return updates

    def rename(self, schema, context, old_name, new_name):
        updaterec = self.table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(
            dbops.Update(
                table=self.table, record=updaterec, condition=condition))

    def delete(self, schema, context, scls):
        self.pgops.add(
            dbops.Delete(
                table=self.table, condition=[('name', str(scls.name))]))


class CreateNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedObjectMetaCommand.apply(self, schema, context)
        updates = self.create_object(schema, obj)
        self.updates = updates
        return obj


class CreateOrAlterNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        existing = schema.get(self.classname, None)

        obj = self.__class__.get_adaptee().apply(self, schema, context)
        self.scls = obj
        NamedObjectMetaCommand.apply(self, schema, context)

        if existing is None:
            updates = self.create_object(schema, obj)
            self.updates = updates
        else:
            self.updates = self.update(schema, context)
        return obj


class RenameNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedObjectMetaCommand.apply(self, schema, context)
        self.rename(schema, context, self.classname, self.new_name)
        return obj


class RebaseNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedObjectMetaCommand.apply(self, schema, context)
        return obj


class AlterNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        NamedObjectMetaCommand.apply(self, schema, context)
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        self.scls = obj
        self.updates = self.update(schema, context)
        return obj


class DeleteNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedObjectMetaCommand.apply(self, schema, context)
        self.delete(schema, context, obj)
        return obj


class AlterObjectProperty(MetaCommand, adapts=sd.AlterObjectProperty):
    pass


class FunctionCommand:
    table = metaschema.get_metaclass_table(s_funcs.Function)

    def get_pgname(self, func: s_funcs.Function):
        return (
            common.edgedb_module_name_to_schema_name(func.shortname.module),
            common.edgedb_name_to_pg_name(func.shortname.name)
        )

    def get_pgtype(self, func: s_funcs.Function, obj, schema):
        try:
            return types.pg_type_from_object(schema, obj)
        except ValueError:
            raise ql_errors.EdgeQLError(
                f'could not compile parameter type {obj!r} '
                f'of function {func.shortname}',
                context=self.source_context) from None

    def compile_default(self, func: s_funcs.Function, default: str, schema):
        try:
            ir = ql_compiler.compile_fragment_to_ir(
                default, schema, location='parameter-default')

            if not irutils.is_const(ir):
                raise ValueError('expression not constant')

            sql_tree = compiler.compile_ir_to_sql_tree(
                ir, schema=schema, singleton_mode=True)
            return codegen.SQLSourceGenerator.to_source(sql_tree)

        except Exception as ex:
            raise ql_errors.EdgeQLError(
                f'could not compile default expression {default!r} '
                f'of function {func.shortname}: {ex}',
                context=self.source_context) from ex

    def compile_args(self, func: s_funcs.Function, schema):
        if not func.paramtypes:
            return

        # TODO: Refactor to move this logic closer to pg_type_from_object,
        # or add another layer of abstraction for typing for complex
        # objects.
        has_anyarray = (
            (isinstance(func.returntype, s_types.Array) and
                func.returntype.element_type.name == 'std::any') or
            any(isinstance(at, s_types.Array) and
                at.element_type.name == 'std::any' for at in func.paramtypes))

        args = []
        for an, at, ad in itertools.zip_longest(func.paramnames,
                                                func.paramtypes,
                                                func.paramdefaults):
            pg_ad = None
            if ad is not None:
                pg_ad = self.compile_default(func, ad, schema)

            if has_anyarray and at.name == 'std::any':
                pg_at = ('anyelement',)
            else:
                pg_at = self.get_pgtype(func, at, schema)

            args.append((an, pg_at, pg_ad))

        return args


class CreateFunction(FunctionCommand, CreateNamedObject,
                     adapts=s_funcs.CreateFunction):

    def compile_sql_function(self, func: s_funcs.Function, schema):
        if func.varparam is not None:
            varparam = func.varparam + 1
        else:
            varparam = None

        return dbops.Function(
            name=self.get_pgname(func),
            args=self.compile_args(func, schema),
            variadic_arg=varparam,
            set_returning=func.set_returning,
            returns=self.get_pgtype(func, func.returntype, schema),
            text=func.code)

    def compile_edgeql_function(self, func: s_funcs.Function, schema):
        arg_types = None
        if func.paramtypes:
            arg_types = {}

            arg_iter = enumerate(
                itertools.zip_longest(func.paramnames, func.paramtypes))

            for ai, (an, at) in arg_iter:
                if an is None:
                    arg_types[str(ai)] = at
                else:
                    arg_types[an] = at

        body_ir = ql_compiler.compile_to_ir(
            func.code, schema, arg_types=arg_types)

        qchunks, argmap, arg_index, query_type, record_info = \
            compiler.compile_ir_to_sql(
                body_ir, schema=schema, ignore_shapes=True)

        if func.varparam is not None:
            varparam = func.varparam + 1
        else:
            varparam = None

        return dbops.Function(
            name=self.get_pgname(func),
            args=self.compile_args(func, schema),
            variadic_arg=varparam,
            returns=self.get_pgtype(func, func.returntype, schema),
            text=''.join(qchunks))

    def apply(self, schema, context):
        func: s_funcs.Function = super().apply(schema, context)

        if func.code is None:
            return func

        if func.language is ql_ast.Language.SQL:
            dbf = self.compile_sql_function(func, schema)
        elif func.language is ql_ast.Language.EdgeQL:
            dbf = self.compile_edgeql_function(func, schema)
        else:
            raise ql_errors.EdgeQLError(
                f'cannot compile function {func.shortname}: '
                f'unsupported language {func.language}',
                context=self.source_context)

        self.pgops.add(dbops.CreateFunction(dbf))
        return func


class RenameFunction(
        FunctionCommand, RenameNamedObject, adapts=s_funcs.RenameFunction):
    pass


class AlterFunction(
        FunctionCommand, AlterNamedObject, adapts=s_funcs.AlterFunction):
    pass


class DeleteFunction(
        FunctionCommand, DeleteNamedObject, adapts=s_funcs.DeleteFunction):

    def apply(self, schema, context):
        func: s_funcs.Function = super().apply(schema, context)

        if func.code:
            # EdgeQL function (not an alias to an SQL function).
            if func.varparam is not None:
                varparam = func.varparam + 1
            else:
                varparam = None

            self.pgops.add(
                dbops.DropFunction(
                    name=self.get_pgname(func),
                    args=self.compile_args(func, schema),
                    variadic_arg=varparam
                )
            )

        return func


class AttributeCommand:
    table = metaschema.get_metaclass_table(s_attrs.Attribute)


class CreateAttribute(
        AttributeCommand, CreateNamedObject,
        adapts=s_attrs.CreateAttribute):
    op_priority = 1


class RenameAttribute(
        AttributeCommand, RenameNamedObject,
        adapts=s_attrs.RenameAttribute):
    pass


class AlterAttribute(
        AttributeCommand, AlterNamedObject, adapts=s_attrs.AlterAttribute):
    pass


class DeleteAttribute(
        AttributeCommand, DeleteNamedObject,
        adapts=s_attrs.DeleteAttribute):
    pass


class AttributeValueCommand(sd.ObjectCommand,
                            metaclass=ReferencedObjectCommandMeta):
    table = metaschema.get_metaclass_table(s_attrs.AttributeValue)
    op_priority = 1

    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)

        if rec:
            value = updates.get('value')
            if value:
                rec.value = pickle.dumps(value)

        return rec, updates


class CreateAttributeValue(
        AttributeValueCommand, CreateOrAlterNamedObject,
        adapts=s_attrs.CreateAttributeValue):
    pass


class RenameAttributeValue(
        AttributeValueCommand, RenameNamedObject,
        adapts=s_attrs.RenameAttributeValue):
    pass


class AlterAttributeValue(
        AttributeValueCommand, AlterNamedObject,
        adapts=s_attrs.AlterAttributeValue):
    pass


class DeleteAttributeValue(
        AttributeValueCommand, DeleteNamedObject,
        adapts=s_attrs.DeleteAttributeValue):
    pass


class ConstraintCommand(sd.ObjectCommand,
                        metaclass=ReferencedObjectCommandMeta):
    table = metaschema.get_metaclass_table(s_constr.Constraint)
    op_priority = 3

    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)

        if rec and False:
            # Write the original locally-defined expression
            # so that when the schema is introspected the
            # correct finalexpr is restored with scls
            # inheritance mechanisms.
            rec.finalexpr = rec.localfinalexpr

        return rec, updates


class CreateConstraint(
        ConstraintCommand, CreateNamedObject,
        adapts=s_constr.CreateConstraint):
    def apply(self, schema, context):
        constraint = super().apply(schema, context)

        subject = constraint.subject

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.create_ops())
            self.pgops.add(op)

        return constraint


class RenameConstraint(
        ConstraintCommand, RenameNamedObject,
        adapts=s_constr.RenameConstraint):
    def apply(self, schema, context):
        constr_ctx = context.get(s_constr.ConstraintCommandContext)
        assert constr_ctx
        orig_constraint = constr_ctx.original_class
        schemac_to_backendc = \
            schemamech.ConstraintMech.schema_constraint_to_backend_constraint
        orig_bconstr = schemac_to_backendc(
            orig_constraint.subject, orig_constraint, schema)

        constraint = super().apply(schema, context)

        subject = constraint.subject

        if subject is not None:
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.rename_ops(orig_bconstr))
            self.pgops.add(op)

        return constraint


class AlterConstraint(
        ConstraintCommand, AlterNamedObject,
        adapts=s_constr.AlterConstraint):
    def _alter_finalize(self, schema, context, constraint):
        super()._alter_finalize(schema, context, constraint)

        subject = constraint.subject
        ctx = context.get(s_constr.ConstraintCommandContext)

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint

            bconstr = schemac_to_backendc(subject, constraint, schema)

            orig_constraint = ctx.original_class
            orig_bconstr = schemac_to_backendc(
                orig_constraint.subject, orig_constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.alter_ops(orig_bconstr))
            self.pgops.add(op)

        return constraint


class DeleteConstraint(
        ConstraintCommand, DeleteNamedObject,
        adapts=s_constr.DeleteConstraint):
    def apply(self, schema, context):
        constraint = super().apply(schema, context)

        subject = constraint.subject

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.delete_ops())
            self.pgops.add(op)

        return constraint


class ViewCapableObjectMetaCommand(NamedObjectMetaCommand):
    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)
        if rec and False:
            expr = updates.get('expr')
            if expr:
                self.pgops.add(
                    deltadbops.MangleExprObjectRefs(
                        scls=self.scls, field='expr', expr=expr, priority=3))

        return rec, updates


class ScalarTypeMetaCommand(ViewCapableObjectMetaCommand):
    table = metaschema.get_metaclass_table(s_scalars.ScalarType)

    def is_sequence(self, schema, scalar):
        seq = schema.get('std::sequence', default=None)
        return seq is not None and scalar.issubclass(seq)

    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)
        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default)

        return rec, updates

    def alter_scalar_type(self, scalar, schema, new_type, intent):

        users = []

        for link in schema.get_objects(type='link'):
            if link.target and link.target.name == scalar.name:
                users.append((link.source, link))

        domain_name = common.scalar_name_to_domain_name(
            scalar.name, catenate=False)

        new_constraints = scalar.local_constraints
        base = types.get_scalar_base(schema, scalar)

        target_type = new_type

        schemac_to_backendc = \
            schemamech.ConstraintMech.\
            schema_constraint_to_backend_constraint

        if intent == 'alter':
            new_name = domain_name[0], domain_name[1] + '_tmp'
            self.pgops.add(dbops.RenameDomain(domain_name, new_name))
            target_type = domain_name

            self.pgops.add(dbops.CreateDomain(
                name=domain_name, base=new_type))

            for constraint in new_constraints.values():
                bconstr = schemac_to_backendc(scalar, constraint, schema)
                op = dbops.CommandGroup(priority=1)
                op.add_command(bconstr.create_ops())
                self.pgops.add(op)

            domain_name = new_name

        elif intent == 'create':
            self.pgops.add(dbops.CreateDomain(name=domain_name, base=base))

        for host_class, item_class in users:
            if isinstance(item_class, s_links.Link):
                name = item_class.shortname
            else:
                name = item_class.name

            table_name = common.get_table_name(host_class, catenate=False)
            column_name = common.edgedb_name_to_pg_name(name)

            alter_type = dbops.AlterTableAlterColumnType(
                column_name, target_type)
            alter_table = dbops.AlterTable(table_name)
            alter_table.add_operation(alter_type)
            self.pgops.add(alter_table)

        for child_scalar in schema.get_objects(type='ScalarType'):
            if [b.name for b in child_scalar.bases] == [scalar.name]:
                self.alter_scalar_type(
                    child_scalar, schema, target_type, 'alter')

        if intent == 'drop':
            self.pgops.add(dbops.DropDomain(domain_name))


class CreateScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.CreateScalarType):
    def apply(self, schema, context=None):
        scalar = s_scalars.CreateScalarType.apply(self, schema, context)
        ScalarTypeMetaCommand.apply(self, schema, context)

        updates = self.create_object(schema, scalar)

        if scalar.is_abstract:
            return scalar

        new_domain_name = common.scalar_name_to_domain_name(
            scalar.name, catenate=False)
        base = types.get_scalar_base(schema, scalar)

        self.pgops.add(dbops.CreateDomain(name=new_domain_name, base=base))

        if self.is_sequence(schema, scalar):
            seq_name = common.scalar_name_to_sequence_name(
                scalar.name, catenate=False)
            self.pgops.add(dbops.CreateSequence(name=seq_name))

        default = updates.get('default')
        if default:
            if (
                    default is not None and
                    not isinstance(default, s_expr.ExpressionText)):
                # We only care to support literal defaults here.  Supporting
                # defaults based on queries has no sense on the database level
                # since the database forbids queries for DEFAULT and pre-
                # calculating the value does not make sense either since the
                # whole point of query defaults is for them to be dynamic.
                self.pgops.add(
                    dbops.AlterDomainAlterDefault(
                        name=new_domain_name, default=default))

        return scalar


class RenameScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RenameScalarType):
    def apply(self, schema, context=None):
        scls = s_scalars.RenameScalarType.apply(self, schema, context)
        ScalarTypeMetaCommand.apply(self, schema, context)

        domain_name = common.scalar_name_to_domain_name(
            self.classname, catenate=False)
        new_domain_name = common.scalar_name_to_domain_name(
            self.new_name, catenate=False)

        self.pgops.add(
            dbops.RenameDomain(name=domain_name, new_name=new_domain_name))
        self.rename(schema, context, self.classname, self.new_name)

        if self.is_sequence(schema, scls):
            seq_name = common.scalar_name_to_sequence_name(
                self.classname, catenate=False)
            new_seq_name = common.scalar_name_to_sequence_name(
                self.new_name, catenate=False)

            self.pgops.add(
                dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return scls


class RebaseScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RebaseScalarType):
    # Rebase is taken care of in AlterScalarType
    pass


class AlterScalarType(ScalarTypeMetaCommand, adapts=s_scalars.AlterScalarType):
    def apply(self, schema, context=None):
        old_scalar = schema.get(self.classname).copy()
        new_scalar = s_scalars.AlterScalarType.apply(self, schema, context)
        ScalarTypeMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(new_scalar.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition))

        self.alter_scalar(
            self, schema, context, old_scalar, new_scalar, updates=updates)

        return new_scalar

    @classmethod
    def alter_scalar(
            cls, op, schema, context, old_scalar, new_scalar, in_place=True,
            updates=None):

        old_base = types.get_scalar_base(schema, old_scalar)
        base = types.get_scalar_base(schema, new_scalar)

        domain_name = common.scalar_name_to_domain_name(
            new_scalar.name, catenate=False)

        new_type = None
        type_intent = 'alter'

        if not new_type and old_base != base:
            new_type = base

        if new_type:
            # The change of the underlying data type for domains is a complex
            # problem. There is no direct way in PostgreSQL to change the base
            # type of a domain. Instead, a new domain must be created, all
            # users of the old domain altered to use the new one, and then the
            # old domain dropped.  Obviously this recurses down to every child
            # domain.
            if in_place:
                op.alter_scalar_type(
                    new_scalar, schema, new_type, intent=type_intent)

        if type_intent != 'drop':
            if updates:
                default_delta = updates.get('default')
                if default_delta:
                    if (default_delta is None or
                            isinstance(default_delta, s_expr.ExpressionText)):
                        new_default = None
                    else:
                        new_default = default_delta

                    adad = dbops.AlterDomainAlterDefault(
                        name=domain_name, default=new_default)
                    op.pgops.add(adad)


class DeleteScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.DeleteScalarType):
    def apply(self, schema, context=None):
        scalar = s_scalars.DeleteScalarType.apply(self, schema, context)
        ScalarTypeMetaCommand.apply(self, schema, context)

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        old_domain_name = common.scalar_name_to_domain_name(
            self.classname, catenate=False)

        # Domain dropping gets low priority since other things may
        # depend on it.
        cond = dbops.DomainExists(old_domain_name)
        ops.add(
            dbops.DropDomain(
                name=old_domain_name, conditions=[cond], priority=3))
        ops.add(
            dbops.Delete(
                table=self.table, condition=[(
                    'name', str(self.classname))]))

        if self.is_sequence(schema, scalar):
            seq_name = common.scalar_name_to_sequence_name(
                self.classname, catenate=False)
            self.pgops.add(dbops.DropSequence(name=seq_name))

        return scalar


class UpdateSearchIndexes(MetaCommand):
    def __init__(self, host, **kwargs):
        super().__init__(**kwargs)
        self.host = host

    def get_index_name(self, host_table_name, language, index_class='default'):
        name = '%s_%s_%s_search_idx' % (
            host_table_name[1], language, index_class)
        return common.edgedb_name_to_pg_name(name)

    def apply(self, schema, context):
        if isinstance(self.host, s_objtypes.ObjectType):
            columns = []

            names = sorted(self.host.pointers.keys())

            for link_name in names:
                for link in self.host.pointers[link_name]:
                    if getattr(link, 'search', None):
                        column_name = common.edgedb_name_to_pg_name(link_name)
                        columns.append(
                            dbops.TextSearchIndexColumn(
                                column_name, link.search.weight, 'english'))

            if columns:
                table_name = common.get_table_name(self.host, catenate=False)

                index_name = self.get_index_name(table_name, 'default')
                index = dbops.TextSearchIndex(
                    name=index_name, table_name=table_name, columns=columns)

                cond = dbops.IndexExists(
                    index_name=(table_name[0], index_name))
                op = dbops.DropIndex(index, conditions=(cond, ))
                self.pgops.add(op)
                op = dbops.CreateIndex(index=index)
                self.pgops.add(op)


class CompositeObjectMetaCommand(NamedObjectMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = None
        self._multicommands = {}
        self.update_search_indexes = None

    def _get_multicommand(
            self, context, cmdtype, object_name, *, priority=0,
            force_new=False, manual=False, cmdkwargs={}):
        key = (object_name, priority, frozenset(cmdkwargs.items()))

        try:
            typecommands = self._multicommands[cmdtype]
        except KeyError:
            typecommands = self._multicommands[cmdtype] = {}

        commands = typecommands.get(key)

        if commands is None or force_new or manual:
            command = cmdtype(object_name, priority=priority, **cmdkwargs)

            if not manual:
                try:
                    commands = typecommands[key]
                except KeyError:
                    commands = typecommands[key] = []

                commands.append(command)
        else:
            command = commands[-1]

        return command

    def _attach_multicommand(self, context, cmdtype):
        try:
            typecommands = self._multicommands[cmdtype]
        except KeyError:
            return
        else:
            commands = list(
                itertools.chain.from_iterable(typecommands.values()))

            if commands:
                commands = sorted(commands, key=lambda i: i.priority)
                self.pgops.update(commands)

    def get_alter_table(
            self, context, priority=0, force_new=False, contained=False,
            manual=False, table_name=None):

        tabname = table_name if table_name else self.table_name

        if not tabname:
            ctx = context.get(self.__class__)
            assert ctx
            tabname = common.get_table_name(ctx.scls, catenate=False)
            if table_name is None:
                self.table_name = tabname

        return self._get_multicommand(
            context, dbops.AlterTable, tabname, priority=priority,
            force_new=force_new, manual=manual,
            cmdkwargs={'contained': contained})

    def attach_alter_table(self, context):
        self._attach_multicommand(context, dbops.AlterTable)

    def rename(self, schema, context, old_name, new_name, obj=None):
        super().rename(schema, context, old_name, new_name)

        if obj is not None and isinstance(obj, s_links.Link):
            old_table_name = common.link_name_to_table_name(
                old_name, catenate=False)
            new_table_name = common.link_name_to_table_name(
                new_name, catenate=False)
        else:
            old_table_name = common.objtype_name_to_table_name(
                old_name, catenate=False)
            new_table_name = common.objtype_name_to_table_name(
                new_name, catenate=False)

        cond = dbops.TableExists(name=old_table_name)

        if old_name.module != new_name.module:
            self.pgops.add(
                dbops.AlterTableSetSchema(
                    old_table_name, new_table_name[0], conditions=(cond, )))
            old_table_name = (new_table_name[0], old_table_name[1])

            cond = dbops.TableExists(name=old_table_name)

        if old_name.name != new_name.name:
            self.pgops.add(
                dbops.AlterTableRenameTo(
                    old_table_name, new_table_name[1], conditions=(cond, )))

    def search_index_add(self, host, pointer, schema, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_alter(self, host, pointer, schema, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_delete(self, host, pointer, schema, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    @classmethod
    def get_source_and_pointer_ctx(cls, schema, context):
        if context:
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            link = context.get(s_links.LinkCommandContext)
        else:
            objtype = link = None

        if objtype:
            source, pointer = objtype, link
        elif link:
            property = context.get(s_props.PropertyCommandContext)
            source, pointer = link, property
        else:
            source = pointer = None

        return source, pointer

    def affirm_pointer_defaults(self, source, schema, context):
        for pointer_name, pointer in source.pointers.items():
            # XXX pointer_storage_info?
            if (
                    pointer.generic() or not pointer.scalar() or
                    not pointer.singular() or not pointer.default):
                continue

            default = None

            if not isinstance(pointer.default, s_expr.ExpressionText):
                default = pointer.default

            if default is not None:
                alter_table = self.get_alter_table(
                    context, priority=3, contained=True)
                column_name = common.edgedb_name_to_pg_name(pointer_name)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=column_name, default=default))

    def adjust_pointer_storage(self, orig_pointer, pointer, schema, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
            orig_pointer, schema=schema)
        new_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=schema)

        old_target = orig_pointer.target
        new_target = pointer.target

        source_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
        source_op = source_ctx.op

        type_change_ok = False

        if (old_target.name != new_target.name or
                old_ptr_stor_info.table_type != new_ptr_stor_info.table_type):

            for op in self.get_subcommands(type=s_scalars.ScalarTypeCommand):
                for rename in op(s_scalars.RenameScalarType):
                    if (old_target.name == rename.classname and
                            new_target.name == rename.new_name):
                        # Our target alter is a mere rename
                        type_change_ok = True

                if isinstance(op, s_scalars.CreateScalarType):
                    if op.classname == new_target.name:
                        # CreateScalarType will take care of everything for us
                        type_change_ok = True

            if old_ptr_stor_info.table_type != new_ptr_stor_info.table_type:
                # The attribute is being moved from one table to another
                opg = dbops.CommandGroup(priority=1)
                at = source_op.get_alter_table(context, manual=True)

                if old_ptr_stor_info.table_type == 'ObjectType':
                    pat = self.get_alter_table(context, manual=True)

                    # Moved from object table to link table
                    col = dbops.Column(
                        name=old_ptr_stor_info.column_name,
                        type=common.qname(*old_ptr_stor_info.column_type))
                    at.add_command(dbops.AlterTableDropColumn(col))

                    newcol = dbops.Column(
                        name=new_ptr_stor_info.column_name,
                        type=common.qname(*new_ptr_stor_info.column_type))

                    cond = dbops.ColumnExists(
                        new_ptr_stor_info.table_name, column_name=newcol.name)

                    pat.add_command(
                        (dbops.AlterTableAddColumn(newcol), None, (cond, )))
                else:
                    otabname = common.get_table_name(
                        orig_pointer, catenate=False)
                    pat = self.get_alter_table(
                        context, manual=True, table_name=otabname)

                    oldcol = dbops.Column(
                        name=old_ptr_stor_info.column_name,
                        type=common.qname(*old_ptr_stor_info.column_type))

                    if oldcol.name != 'std::target':
                        pat.add_command(dbops.AlterTableDropColumn(oldcol))

                    # Moved from link to object
                    cols = self.get_columns(pointer, schema)

                    for col in cols:
                        cond = dbops.ColumnExists(
                            new_ptr_stor_info.table_name, column_name=col.name)
                        op = (dbops.AlterTableAddColumn(col), None, (cond, ))
                        at.add_operation(op)

                opg.add_command(at)
                opg.add_command(pat)

                self.pgops.add(opg)

            else:
                if old_target != new_target and not type_change_ok:
                    if isinstance(old_target, s_scalars.ScalarType):
                        AlterScalarType.alter_scalar(
                            self, schema, context, old_target, new_target,
                            in_place=False)

                        alter_table = source_op.get_alter_table(
                            context, priority=1)

                        new_type = \
                            types.pg_type_from_object(schema, new_target)

                        alter_type = dbops.AlterTableAlterColumnType(
                            old_ptr_stor_info.column_name,
                            common.qname(*new_type))
                        alter_table.add_operation(alter_type)

    def apply_base_delta(self, orig_source, source, schema, context):
        db_ctx = context.get(s_db.DatabaseCommandContext)
        orig_source.bases = [
            db_ctx.op._renames.get(b, b) for b in orig_source.bases
        ]

        dropped_bases = {b.name
                         for b in orig_source.bases
                         } - {b.name
                              for b in source.bases}

        if isinstance(source, s_objtypes.ObjectType):
            nameconv = common.objtype_name_to_table_name
            source_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
            ptr_cmd = s_links.CreateLink
        else:
            nameconv = common.link_name_to_table_name
            source_ctx = context.get(s_links.LinkCommandContext)
            ptr_cmd = s_props.CreateProperty

        alter_table = source_ctx.op.get_alter_table(context, force_new=True)

        if (isinstance(source, s_objtypes.ObjectType) or
                source_ctx.op.has_table(source, schema)):

            source.acquire_ancestor_inheritance(schema)
            orig_source.acquire_ancestor_inheritance(schema)

            created_ptrs = set()
            for ptr in source_ctx.op.get_subcommands(type=ptr_cmd):
                created_ptrs.add(ptr.classname)

            inherited_aptrs = set()

            for base in source.bases:
                for ptr in base.pointers.values():
                    if ptr.scalar():
                        inherited_aptrs.add(ptr.shortname)

            added_inh_ptrs = inherited_aptrs - {
                p.shortname
                for p in orig_source.pointers.values()
            }

            for added_ptr in added_inh_ptrs - created_ptrs:
                ptr = source.pointers[added_ptr]
                ptr_stor_info = types.get_pointer_storage_info(
                    ptr, schema=schema)

                is_a_column = ((
                    ptr_stor_info.table_type == 'ObjectType' and
                    isinstance(source, s_objtypes.ObjectType)) or (
                        ptr_stor_info.table_type == 'link' and
                        isinstance(source, s_links.Link)))

                if is_a_column:
                    col = dbops.Column(
                        name=ptr_stor_info.column_name,
                        type=common.qname(*ptr_stor_info.column_type),
                        required=ptr.required)
                    cond = dbops.ColumnExists(
                        table_name=source_ctx.op.table_name,
                        column_name=ptr_stor_info.column_name)
                    alter_table.add_operation(
                        (dbops.AlterTableAddColumn(col), None, (cond, )))

            if dropped_bases:
                alter_table_drop_parent = source_ctx.op.get_alter_table(
                    context, force_new=True)

                for dropped_base in dropped_bases:
                    parent_table_name = nameconv(
                        sn.Name(dropped_base), catenate=False)
                    op = dbops.AlterTableDropParent(
                        parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation(op)

                dropped_ptrs = set(orig_source.pointers) - set(source.pointers)

                if dropped_ptrs:
                    alter_table_drop_ptr = source_ctx.op.get_alter_table(
                        context, force_new=True)

                    for dropped_ptr in dropped_ptrs:
                        ptr = orig_source.pointers[dropped_ptr]
                        ptr_stor_info = types.get_pointer_storage_info(
                            ptr, schema=schema)

                        is_a_column = ((
                            ptr_stor_info.table_type == 'ObjectType' and
                            isinstance(source, s_objtypes.ObjectType)) or (
                                ptr_stor_info.table_type == 'link' and
                                isinstance(source, s_links.Link)))

                        if is_a_column:
                            col = dbops.Column(
                                name=ptr_stor_info.column_name,
                                type=common.qname(*ptr_stor_info.column_type),
                                required=ptr.required)

                            cond = dbops.ColumnExists(
                                table_name=ptr_stor_info.table_name,
                                column_name=ptr_stor_info.column_name)
                            op = dbops.AlterTableDropColumn(col)
                            alter_table_drop_ptr.add_command(
                                (op, (cond, ), ()))

            current_bases = list(
                ordered.OrderedSet(b.name for b in orig_source.bases) -
                dropped_bases)

            new_bases = [b.name for b in source.bases]

            unchanged_order = list(
                itertools.takewhile(
                    lambda x: x[0] == x[1], zip(current_bases, new_bases)))

            old_base_order = current_bases[len(unchanged_order):]
            new_base_order = new_bases[len(unchanged_order):]

            if new_base_order:
                table_name = nameconv(source.name, catenate=False)
                alter_table_drop_parent = source_ctx.op.get_alter_table(
                    context, force_new=True)
                alter_table_add_parent = source_ctx.op.get_alter_table(
                    context, force_new=True)

                for base in old_base_order:
                    parent_table_name = nameconv(sn.Name(base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableDropParent(
                        parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation((op, [cond], None))

                for added_base in new_base_order:
                    parent_table_name = nameconv(
                        sn.Name(added_base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableAddParent(
                        parent_name=parent_table_name)
                    alter_table_add_parent.add_operation((op, None, [cond]))


class SourceIndexCommand(sd.ObjectCommand,
                         metaclass=ReferencedObjectCommandMeta):
    table = metaschema.get_metaclass_table(s_indexes.SourceIndex)


class CreateSourceIndex(SourceIndexCommand, CreateNamedObject,
                        adapts=s_indexes.CreateSourceIndex):

    def apply(self, schema, context):
        index = CreateNamedObject.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_objtypes.ObjectTypeCommandContext)
        table_name = common.get_table_name(source.scls, catenate=False)
        ir = ql_compiler.compile_fragment_to_ir(
            index.expr, schema, location='selector')

        sql_tree = compiler.compile_ir_to_sql_tree(
            ir, schema=schema, singleton_mode=True)
        sql_expr = codegen.SQLSourceGenerator.to_source(sql_tree)

        if isinstance(sql_tree, pg_ast.ImplicitRowExpr):
            # Trim the parentheses to avoid PostgreSQL choking on double
            # parentheses. since it expects only a single set around the column
            # list.
            sql_expr = sql_expr[1:-1]
        index_name = '{}_reg_idx'.format(index.name)
        pg_index = dbops.Index(
            name=index_name, table_name=table_name, expr=sql_expr,
            unique=False, inherit=True, metadata={'schemaname': index.name})
        self.pgops.add(dbops.CreateIndex(pg_index, priority=3))

        return index


class RenameSourceIndex(SourceIndexCommand, RenameNamedObject,
                        adapts=s_indexes.RenameSourceIndex):

    def apply(self, schema, context):
        index = s_indexes.RenameSourceIndex.apply(self, schema, context)
        RenameNamedObject.apply(self, schema, context)

        subject = context.get(s_links.LinkCommandContext)
        if not subject:
            subject = context.get(s_objtypes.ObjectTypeCommandContext)
        orig_table_name = common.get_table_name(
            subject.original_class, catenate=False)

        index_ctx = context.get(s_indexes.SourceIndexCommandContext)
        new_index_name = '{}_reg_idx'.format(index.name)

        orig_idx = index_ctx.original_class
        orig_idx_name = '{}_reg_idx'.format(orig_idx.name)
        orig_pg_idx = dbops.Index(
            name=orig_idx_name, table_name=orig_table_name, inherit=True,
            metadata={'schemaname': index.name})

        rename = dbops.RenameIndex(orig_pg_idx, new_name=new_index_name)
        self.pgops.add(rename)

        return index


class AlterSourceIndex(SourceIndexCommand, AlterNamedObject,
                       adapts=s_indexes.AlterSourceIndex):
    def apply(self, schema, context=None):
        result = s_indexes.AlterSourceIndex.apply(self, schema, context)
        AlterNamedObject.apply(self, schema, context)
        return result


class DeleteSourceIndex(SourceIndexCommand, DeleteNamedObject,
                        adapts=s_indexes.DeleteSourceIndex):

    def apply(self, schema, context=None):
        index = s_indexes.DeleteSourceIndex.apply(self, schema, context)
        DeleteNamedObject.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_objtypes.ObjectTypeCommandContext)

        if not isinstance(source.op, s_named.DeleteNamedObject):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            #
            table_name = common.get_table_name(source.scls, catenate=False)
            index_name = '{}_reg_idx'.format(index.name)
            index = dbops.Index(
                name=index_name, table_name=table_name, inherit=True)
            index_exists = dbops.IndexExists(
                (table_name[0], index.name_in_catalog))
            self.pgops.add(
                dbops.DropIndex(
                    index, priority=3, conditions=(index_exists, )))

        return index


class ObjectTypeMetaCommand(ViewCapableObjectMetaCommand,
                            CompositeObjectMetaCommand):
    @property
    def table(self):
        if self.scls.is_virtual:
            mcls = s_objtypes.UnionObjectType
        elif self.scls.is_derived:
            mcls = s_objtypes.DerivedObjectType
        else:
            mcls = s_objtypes.ObjectType

        return metaschema.get_metaclass_table(mcls)

    @classmethod
    def has_table(cls, objtype, schema):
        return not (objtype.is_virtual or objtype.is_derived)


class CreateObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.CreateObjectType):
    def apply(self, schema, context=None):
        objtype_props = self.get_struct_properties(schema)
        is_virtual = objtype_props.get('is_virtual')
        is_derived = objtype_props.get('is_derived')
        if is_virtual or is_derived:
            objtype = s_objtypes.CreateObjectType.apply(self, schema, context)
            self.create_object(schema, objtype)
            return objtype

        new_table_name = common.objtype_name_to_table_name(
            self.classname, catenate=False)
        self.table_name = new_table_name

        columns = []
        if objtype_props.get('name') == 'std::Object':
            token_col = dbops.Column(
                name='__edb_token', type='uuid', required=False)
            columns.append(token_col)

        objtype_table = dbops.Table(name=new_table_name, columns=columns)
        self.pgops.add(dbops.CreateTable(table=objtype_table))

        alter_table = self.get_alter_table(context)

        objtype = s_objtypes.CreateObjectType.apply(self, schema, context)
        ObjectTypeMetaCommand.apply(self, schema, context)

        fields = self.create_object(schema, objtype)

        if objtype.name.module != 'schema':
            constr_name = common.edgedb_name_to_pg_name(
                self.classname + '.class_check')

            constr_expr = dbops.Query("""
                SELECT '"std::__type__" = ' || quote_literal(id)
                FROM edgedb.ObjectType WHERE name = $1
            """, [objtype.name], type='text')

            cid_constraint = dbops.CheckConstraint(
                self.table_name, constr_name, constr_expr, inherit=False)
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(cid_constraint))

            cid_col = dbops.Column(
                name='std::__type__', type='uuid', required=True)

            if objtype.name == 'std::Object':
                alter_table.add_operation(dbops.AlterTableAddColumn(cid_col))

            constraint = dbops.PrimaryKey(
                table_name=alter_table.name, columns=['std::id'])
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(constraint))

        cntn = common.objtype_name_to_table_name

        bases = (
            dbops.Table(name=cntn(sn.Name(p), catenate=False))
            for p in fields['bases']
        )
        objtype_table.add_bases(bases)

        self.affirm_pointer_defaults(objtype, schema, context)

        self.attach_alter_table(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.pgops.add(
            dbops.Comment(object=objtype_table, text=self.classname))

        return objtype


class RenameObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RenameObjectType):
    def apply(self, schema, context=None):
        scls = s_objtypes.RenameObjectType.apply(self, schema, context)
        ObjectTypeMetaCommand.apply(self, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        assert objtype

        db_ctx = context.get(s_db.DatabaseCommandContext)
        assert db_ctx

        db_ctx.op._renames[objtype.original_class] = scls

        objtype.op.attach_alter_table(context)

        self.rename(schema, context, self.classname, self.new_name)

        new_table_name = common.objtype_name_to_table_name(
            self.new_name, catenate=False)
        objtype_table = dbops.Table(name=new_table_name)
        self.pgops.add(dbops.Comment(object=objtype_table, text=self.new_name))

        objtype.op.table_name = common.objtype_name_to_table_name(
            self.new_name, catenate=False)

        # Need to update all bits that reference objtype name

        old_constr_name = common.edgedb_name_to_pg_name(
            self.classname + '.class_check')
        new_constr_name = common.edgedb_name_to_pg_name(
            self.new_name + '.class_check')

        alter_table = self.get_alter_table(context, manual=True)
        rc = dbops.AlterTableRenameConstraintSimple(
            alter_table.name, old_name=old_constr_name,
            new_name=new_constr_name)
        self.pgops.add(rc)

        self.table_name = common.objtype_name_to_table_name(
            self.new_name, catenate=False)

        objtype.original_class.name = scls.name

        return scls


class RebaseObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RebaseObjectType):
    def apply(self, schema, context):
        result = s_objtypes.RebaseObjectType.apply(self, schema, context)
        ObjectTypeMetaCommand.apply(self, schema, context)

        if self.has_table(result, schema):
            objtype_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
            source = objtype_ctx.scls
            orig_source = objtype_ctx.original_class
            self.apply_base_delta(orig_source, source, schema, context)

        return result


class AlterObjectType(ObjectTypeMetaCommand,
                      adapts=s_objtypes.AlterObjectType):
    def apply(self, schema, context=None):
        self.table_name = common.objtype_name_to_table_name(
            self.classname, catenate=False)
        objtype = s_objtypes.AlterObjectType.apply(
            self, schema, context=context)
        ObjectTypeMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(objtype.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition))

        if self.has_table(objtype, schema):
            self.attach_alter_table(context)

            if self.update_search_indexes:
                self.update_search_indexes.apply(schema, context)
                self.pgops.add(self.update_search_indexes)

        return objtype


class DeleteObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.DeleteObjectType):
    def apply(self, schema, context=None):
        old_table_name = common.objtype_name_to_table_name(
            self.classname, catenate=False)

        objtype = s_objtypes.DeleteObjectType.apply(self, schema, context)
        ObjectTypeMetaCommand.apply(self, schema, context)

        self.delete(schema, context, objtype)

        if self.has_table(objtype, schema):
            self.pgops.add(dbops.DropTable(name=old_table_name, priority=3))

        return objtype


class ActionCommand:
    table = metaschema.get_metaclass_table(s_policy.Action)


class CreateAction(
        CreateNamedObject, ActionCommand, adapts=s_policy.CreateAction):
    pass


class RenameAction(
        RenameNamedObject, ActionCommand, adapts=s_policy.RenameAction):
    pass


class AlterAction(
        AlterNamedObject, ActionCommand, adapts=s_policy.AlterAction):
    pass


class DeleteAction(
        DeleteNamedObject, ActionCommand, adapts=s_policy.DeleteAction):
    pass


class EventCommand(metaclass=CommandMeta):
    table = metaschema.get_metaclass_table(s_policy.Event)


class CreateEvent(
        EventCommand, CreateNamedObject, adapts=s_policy.CreateEvent):
    pass


class RenameEvent(
        EventCommand, RenameNamedObject, adapts=s_policy.RenameEvent):
    pass


class RebaseEvent(
        EventCommand, RebaseNamedObject, adapts=s_policy.RebaseEvent):
    pass


class AlterEvent(
        EventCommand, AlterNamedObject, adapts=s_policy.AlterEvent):
    pass


class DeleteEvent(
        EventCommand, DeleteNamedObject, adapts=s_policy.DeleteEvent):
    pass


class PolicyCommand(sd.ObjectCommand, metaclass=ReferencedObjectCommandMeta):
    table = metaschema.get_metaclass_table(s_policy.Policy)
    op_priority = 2


class CreatePolicy(
        PolicyCommand, CreateNamedObject, adapts=s_policy.CreatePolicy):
    pass


class RenamePolicy(
        PolicyCommand, RenameNamedObject, adapts=s_policy.RenamePolicy):
    pass


class AlterPolicy(
        PolicyCommand, AlterNamedObject, adapts=s_policy.AlterPolicy):
    pass


class DeletePolicy(
        PolicyCommand, DeleteNamedObject, adapts=s_policy.DeletePolicy):
    pass


class SchedulePointerCardinalityUpdate(MetaCommand):
    pass


class CancelPointerCardinalityUpdate(MetaCommand):
    pass


class PointerMetaCommand(MetaCommand, sd.ObjectCommand,
                         metaclass=ReferencedObjectCommandMeta):
    def get_host(self, schema, context):
        if context:
            link = context.get(s_links.LinkCommandContext)
            if link and isinstance(self, s_props.PropertyCommand):
                return link
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            if objtype:
                return objtype

    def record_metadata(self, pointer, old_pointer, schema, context):
        rec, updates = self.fill_record(schema)

        if updates:
            if not rec:
                rec = self.table.record()

        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default)

        return rec, updates

    def alter_host_table_column(
            self, old_ptr, ptr, schema, context, old_type, new_type):

        dropped_scalar = None

        for op in self.get_subcommands(type=s_scalars.ScalarTypeCommand):
            for rename in op(s_scalars.RenameScalarType):
                if (old_type == rename.classname and
                        new_type == rename.new_name):
                    # Our target alter is a mere rename
                    return
            if isinstance(op, s_scalars.CreateScalarType):
                if op.classname == new_type:
                    # CreateScalarType will take care of everything for us
                    return
            elif isinstance(op, s_scalars.DeleteScalarType):
                if op.classname == old_type:
                    # The former target scalar might as well have been dropped
                    dropped_scalar = op.old_class

        old_target = schema.get(old_type, dropped_scalar)
        assert old_target
        new_target = schema.get(new_type)

        alter_table = context.get(
            s_objtypes.ObjectTypeCommandContext).op.get_alter_table(
                context, priority=1)
        column_name = common.edgedb_name_to_pg_name(ptr.shortname)

        if isinstance(new_target, s_scalars.ScalarType):
            target_type = types.pg_type_from_object(schema, new_target)

            if isinstance(old_target, s_scalars.ScalarType):
                AlterScalarType.alter_scalar(
                    self, schema, context, old_target, new_target,
                    in_place=False)
                alter_type = dbops.AlterTableAlterColumnType(
                    column_name, common.qname(*target_type))
                alter_table.add_operation(alter_type)
            else:
                cols = self.get_columns(ptr, schema)
                ops = [dbops.AlterTableAddColumn(col) for col in cols]
                for op in ops:
                    alter_table.add_operation(op)
        else:
            col = dbops.Column(name=column_name, type='text')
            alter_table.add_operation(dbops.AlterTableDropColumn(col))

    def get_pointer_default(self, ptr, schema, context):
        if ptr.is_pure_computable():
            return None

        default = self.updates.get('default')
        default_value = None

        if default is not None:
            if isinstance(default, s_expr.ExpressionText):
                default_value = schemamech.ptr_default_to_col_default(
                    schema, ptr, default)
            else:
                default_value = common.quote_literal(
                    str(default))
        elif ptr.target.issubclass(schema.get('std::sequence')):
            # TODO: replace this with a generic scalar type default
            #       using std::nextval().
            seq_name = common.quote_literal(
                common.scalar_name_to_sequence_name(ptr.target.name))
            default_value = f'nextval({seq_name}::regclass)'

        return default_value

    def alter_pointer_default(self, pointer, schema, context):
        default = self.updates.get('default')
        if default:
            new_default = None
            have_new_default = True

            if not default:
                new_default = None
            else:
                if not isinstance(default, s_expr.ExpressionText):
                    new_default = default
                else:
                    have_new_default = False

            if have_new_default:
                source_ctx, pointer_ctx = \
                    CompositeObjectMetaCommand.get_source_and_pointer_ctx(
                        schema, context)
                alter_table = source_ctx.op.get_alter_table(
                    context, contained=True, priority=3)
                column_name = common.edgedb_name_to_pg_name(
                    pointer.shortname)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=column_name, default=new_default))

    @classmethod
    def get_columns(cls, pointer, schema, default=None):
        ptr_stor_info = types.get_pointer_storage_info(pointer, schema=schema)
        col_type = list(ptr_stor_info.column_type)
        if col_type[-1].endswith('[]'):
            # Array
            col_type[-1] = col_type[-1][:-2]
            col_type = common.qname(*col_type) + '[]'
        else:
            col_type = common.qname(*col_type)

        return [
            dbops.Column(
                name=ptr_stor_info.column_name,
                type=col_type,
                required=pointer.required, default=default,
                comment=pointer.shortname)
        ]

    def rename_pointer(self, pointer, schema, context, old_name, new_name):
        if context:
            old_name = pointer.get_shortname(old_name)
            new_name = pointer.get_shortname(new_name)

            host = self.get_host(schema, context)

            if host and old_name != new_name:
                if (new_name.endswith('std::source') and
                        not host.scls.generic()):
                    pass
                else:
                    old_col_name = common.edgedb_name_to_pg_name(old_name)
                    new_col_name = common.edgedb_name_to_pg_name(new_name)

                    ptr_stor_info = types.get_pointer_storage_info(
                        pointer, schema=schema)

                    is_a_column = ((
                        ptr_stor_info.table_type == 'ObjectType' and
                        isinstance(host.scls, s_objtypes.ObjectType)) or (
                            ptr_stor_info.table_type == 'link' and
                            isinstance(host.scls, s_links.Link)))

                    if is_a_column:
                        table_name = common.get_table_name(
                            host.scls, catenate=False)
                        cond = [
                            dbops.ColumnExists(
                                table_name=table_name,
                                column_name=old_col_name)
                        ]
                        rename = dbops.AlterTableRenameColumn(
                            table_name, old_col_name, new_col_name,
                            conditions=cond)
                        self.pgops.add(rename)

                        tabcol = dbops.TableColumn(
                            table_name=table_name, column=dbops.Column(
                                name=new_col_name, type='str'))
                        self.pgops.add(dbops.Comment(tabcol, new_name))

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(
            dbops.Update(
                table=self.table, record=rec, condition=[(
                    'name', str(self.classname))], priority=1))

    @classmethod
    def has_table(cls, src, schema):
        if isinstance(src, s_objtypes.ObjectType):
            return True
        elif src.is_pure_computable() or src.is_derived:
            return False
        elif src.generic():
            if src.name == 'std::link':
                return True
            elif src.has_user_defined_properties():
                return True
            else:
                for l in src.children(schema):
                    if not l.generic():
                        ptr_stor_info = types.get_pointer_storage_info(
                            l, resolve_type=False)
                        if ptr_stor_info.table_type == 'link':
                            return True

                return False
        else:
            return (not src.scalar() or not src.singular() or
                    src.has_user_defined_properties())

    def create_table(self, ptr, schema, context, conditional=False):
        c = self._create_table(ptr, schema, context, conditional=conditional)
        self.pgops.add(c)

    def provide_table(self, ptr, schema, context):
        if not ptr.generic():
            gen_ptr = ptr.bases[0]

            if self.has_table(gen_ptr, schema):
                self.create_table(gen_ptr, schema, context, conditional=True)

        if self.has_table(ptr, schema):
            self.create_table(ptr, schema, context, conditional=True)


class LinkMetaCommand(CompositeObjectMetaCommand, PointerMetaCommand):
    table = metaschema.get_metaclass_table(s_links.Link)

    @classmethod
    def _create_table(
            cls, link, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_table_name(link, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('std::source')
        tgt_col = common.edgedb_name_to_pg_name('std::target')

        if link.name == 'std::link':
            columns.append(
                dbops.Column(
                    name=src_col, type='uuid', required=True,
                    comment='std::source'))
            columns.append(
                dbops.Column(
                    name=tgt_col, type='uuid', required=False,
                    comment='std::target'))
            columns.append(
                dbops.Column(
                    name='ptr_item_id', type='uuid', required=True))

        constraints.append(
            dbops.UniqueConstraint(
                table_name=new_table_name,
                columns=[src_col, tgt_col, 'ptr_item_id']))

        if not link.generic() and link.scalar():
            try:
                tgt_prop = link.pointers['std::target']
            except KeyError:
                pass
            else:
                tgt_ptr = types.get_pointer_storage_info(
                    tgt_prop, schema=schema)
                columns.append(
                    dbops.Column(
                        name=tgt_ptr.column_name,
                        type=common.qname(*tgt_ptr.column_type)))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if link.bases:
            bases = []

            for parent in link.bases:
                if isinstance(parent, s_obj.Object):
                    if create_bases:
                        bc = cls._create_table(
                            parent, schema, context, conditional=True,
                            create_children=False)
                        create_c.add_command(bc)

                    tabname = common.get_table_name(parent, catenate=False)
                    bases.append(dbops.Table(name=tabname))

            table.add_bases(bases)

        ct = dbops.CreateTable(table=table)

        index_name = common.edgedb_name_to_pg_name(
            str(link.name) + 'target_id_default_idx')
        index = dbops.Index(index_name, new_table_name, unique=False)
        index.add_columns([tgt_col])
        ci = dbops.CreateIndex(index)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(dbops.Comment(table, link.name))

        create_c.add_command(c)

        if create_children:
            for l_descendant in link.descendants(schema):
                if cls.has_table(l_descendant, schema):
                    lc = LinkMetaCommand._create_table(
                        l_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
                    create_c.add_command(lc)

        return create_c

    def schedule_mapping_update(self, link, schema, context):
        if self.has_table(link, schema):
            mapping_indexes = context.get(
                s_db.DatabaseCommandContext).op.update_mapping_indexes
            ops = mapping_indexes.links.get(link.name)
            if not ops:
                mapping_indexes.links[link.name] = ops = []
            ops.append((self, link))
            self.pgops.add(SchedulePointerCardinalityUpdate())

    def cancel_mapping_update(self, link, schema, context):
        mapping_indexes = context.get(
            s_db.DatabaseCommandContext).op.update_mapping_indexes
        mapping_indexes.links.pop(link.name, None)
        self.pgops.add(CancelPointerCardinalityUpdate())


class CreateLink(LinkMetaCommand, adapts=s_links.CreateLink):
    def apply(self, schema, context=None):
        # Need to do this early, since potential table alters triggered by
        # sub-commands need this.
        link = s_links.CreateLink.apply(self, schema, context)
        self.table_name = common.get_table_name(link, catenate=False)
        LinkMetaCommand.apply(self, schema, context)

        # We do not want to create a separate table for scalar links, unless
        # they have properties, or are non-singular, since those are stored
        # directly in the source table.
        #
        # Implicit derivative links also do not get their own table since
        # they're just a special case of the parent.
        #
        # On the other hand, much like with objects we want all other links
        # to be in separate tables even if they do not define additional
        # properties. This is to allow for further schema evolution.
        #
        self.provide_table(link, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        rec, updates = self.record_metadata(link, None, schema, context)
        self.updates = updates

        if not link.generic():
            ptr_stor_info = types.get_pointer_storage_info(
                link, resolve_type=False)

            if ptr_stor_info.table_type == 'ObjectType':
                default_value = self.get_pointer_default(link, schema, context)

                cols = self.get_columns(link, schema, default_value)
                table_name = common.get_table_name(
                    objtype.scls, catenate=False)
                objtype_alter_table = objtype.op.get_alter_table(context)

                for col in cols:
                    # The column may already exist as inherited from parent
                    # table.
                    cond = dbops.ColumnExists(
                        table_name=table_name, column_name=col.name)
                    cmd = dbops.AlterTableAddColumn(col)
                    objtype_alter_table.add_operation((cmd, None, (cond, )))

                if default_value is not None:
                    self.alter_pointer_default(link, schema, context)

                search = self.updates.get('search')
                if search:
                    objtype.op.search_index_add(
                        objtype.scls, link, schema, context)

        if link.generic():
            self.affirm_pointer_defaults(link, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        self.pgops.add(
            dbops.Insert(table=self.table, records=[rec], priority=1))

        self.attach_alter_table(context)

        if not link.generic(
        ) and link.cardinality != s_pointers.PointerCardinality.ManyToMany:
            self.schedule_mapping_update(link, schema, context)

        return link


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    def apply(self, schema, context=None):
        result = s_links.RenameLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        self.rename_pointer(
            result, schema, context, self.classname, self.new_name)

        self.attach_alter_table(context)

        if result.generic():
            link_cmd = context.get(s_links.LinkCommandContext)
            assert link_cmd

            self.rename(
                schema, context, self.classname, self.new_name,
                obj=result)
            link_cmd.op.table_name = common.link_name_to_table_name(
                self.new_name, catenate=False)
        else:
            link_cmd = context.get(s_links.LinkCommandContext)

            if self.has_table(result, schema):
                self.rename(
                    schema, context, self.classname, self.new_name,
                    obj=result)

        return result


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def apply(self, schema, context):
        result = s_links.RebaseLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        result.acquire_ancestor_inheritance(schema)

        link_ctx = context.get(s_links.LinkCommandContext)
        source = link_ctx.scls

        orig_source = link_ctx.original_class

        if self.has_table(source, schema):
            self.apply_base_delta(orig_source, source, schema, context)

        return result


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def apply(self, schema, context=None):
        self.old_link = old_link = schema.get(self.classname).copy()
        link = s_links.AlterLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        with context(s_links.LinkCommandContext(self, link)):
            rec, updates = self.record_metadata(
                link, old_link, schema, context)
            self.updates = updates

            self.provide_table(link, schema, context)

            if rec:
                self.pgops.add(
                    dbops.Update(
                        table=self.table, record=rec, condition=[(
                            'name', str(link.name))], priority=1))

            new_type = None
            for op in self.get_subcommands(type=sd.AlterObjectProperty):
                if op.property == 'target':
                    new_type = op.new_value.classname \
                        if op.new_value is not None else None
                    break

            if new_type:
                if not isinstance(link.target, s_obj.Object):
                    link.target = schema.get(link.target)

            self.attach_alter_table(context)

            if not link.generic():
                self.adjust_pointer_storage(old_link, link, schema, context)

                old_ptr_stor_info = types.get_pointer_storage_info(
                    old_link, schema=schema)
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)
                if (
                        old_ptr_stor_info.table_type == 'ObjectType' and
                        ptr_stor_info.table_type == 'ObjectType' and
                        link.required != self.old_link.required):
                    ot_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
                    alter_table = ot_ctx.op.get_alter_table(context)
                    column_name = common.edgedb_name_to_pg_name(
                        link.shortname)
                    alter_table.add_operation(
                        dbops.AlterTableAlterColumnNull(
                            column_name=column_name, null=not link.required))

                search = self.updates.get('search')
                if search:
                    objtype = context.get(s_objtypes.ObjectTypeCommandContext)
                    objtype.op.search_index_add(
                        objtype.scls, link, schema, context)

            if isinstance(link.target, s_scalars.ScalarType):
                self.alter_pointer_default(link, schema, context)

            if not link.generic() and old_link.cardinality != link.cardinality:
                self.schedule_mapping_update(link, schema, context)

        return link


class DeleteLink(LinkMetaCommand, adapts=s_links.DeleteLink):
    def apply(self, schema, context=None):
        result = s_links.DeleteLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        if not result.generic():
            ptr_stor_info = types.get_pointer_storage_info(
                result, schema=schema)
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)

            name = result.shortname

            if ptr_stor_info.table_type == 'ObjectType':
                # Only drop the column if the link was not reinherited in the
                # same delta.
                if name not in objtype.scls.pointers:
                    # This must be a separate so that objects depending
                    # on this column can be dropped correctly.
                    #
                    alter_table = objtype.op.get_alter_table(
                        context, manual=True, priority=2)
                    col = dbops.Column(
                        name=ptr_stor_info.column_name,
                        type=common.qname(*ptr_stor_info.column_type))
                    cond = dbops.ColumnExists(
                        table_name=objtype.op.table_name, column_name=col.name)
                    col = dbops.AlterTableDropColumn(col)
                    alter_table.add_operation((col, [cond], []))
                    self.pgops.add(alter_table)

        old_table_name = common.get_table_name(result, catenate=False)
        condition = dbops.TableExists(name=old_table_name)
        self.pgops.add(
            dbops.DropTable(name=old_table_name, conditions=[condition]))
        self.cancel_mapping_update(result, schema, context)

        if not result.generic(
        ) and result.cardinality != s_pointers.PointerCardinality.ManyToMany:
            self.schedule_mapping_update(result, schema, context)

        self.pgops.add(
            dbops.Delete(
                table=self.table, condition=[('name', str(result.name))]))

        return result


class PropertyMetaCommand(NamedObjectMetaCommand, PointerMetaCommand):
    table = metaschema.get_metaclass_table(s_props.Property)

    @classmethod
    def _create_table(
            cls, prop, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_table_name(prop, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('std::source')

        if prop.name == 'std::property':
            columns.append(
                dbops.Column(
                    name=src_col, type='uuid', required=True,
                    comment='std::source'))
            columns.append(
                dbops.Column(
                    name='ptr_item_id', type='uuid', required=True))

        index_name = common.convert_name(prop.name, 'idx0', catenate=True)

        pg_index = dbops.Index(
            name=index_name, table_name=new_table_name,
            unique=False, columns=[src_col])

        ci = dbops.CreateIndex(pg_index)

        if not prop.generic():
            tgt_cols = cls.get_columns(prop, schema, None)
            columns.extend(tgt_cols)

            constraints.append(
                dbops.UniqueConstraint(
                    table_name=new_table_name,
                    columns=[src_col, 'ptr_item_id'] +
                            [tgt_col.name for tgt_col in tgt_cols]
                )
            )

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if prop.bases:
            bases = []

            for parent in prop.bases:
                if isinstance(parent, s_obj.Object):
                    if create_bases:
                        bc = cls._create_table(
                            parent, schema, context, conditional=True,
                            create_children=False)
                        create_c.add_command(bc)

                    tabname = common.get_table_name(parent, catenate=False)
                    bases.append(dbops.Table(name=tabname))

            table.add_bases(bases)

        ct = dbops.CreateTable(table=table)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(dbops.Comment(table, prop.name))

        create_c.add_command(c)

        if create_children:
            for p_descendant in prop.descendants(schema):
                if cls.has_table(p_descendant, schema):
                    pc = PropertyMetaCommand._create_table(
                        p_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
                    create_c.add_command(pc)

        return create_c


class CreateProperty(PropertyMetaCommand, adapts=s_props.CreateProperty):
    def apply(self, schema, context):
        prop = s_props.CreateProperty.apply(self, schema, context)
        PropertyMetaCommand.apply(self, schema, context)

        src = context.get(s_sources.SourceCommandContext)

        self.provide_table(prop, schema, context)

        with context(s_props.PropertyCommandContext(self, prop)):
            rec, updates = self.record_metadata(prop, None, schema, context)
            self.updates = updates

        if src and self.has_table(src.scls, schema):
            if isinstance(src, s_links.Link):
                src.op.provide_table(src.scls, schema, context)

            ptr_stor_info = types.get_pointer_storage_info(
                prop, resolve_type=False)

            if (not isinstance(src.scls, s_objtypes.ObjectType) or
                    ptr_stor_info.table_type == 'ObjectType'):
                alter_table = src.op.get_alter_table(context)

                default_value = self.get_pointer_default(prop, schema, context)

                cols = self.get_columns(prop, schema, default_value)

                for col in cols:
                    # The column may already exist as inherited from
                    # parent table
                    cond = dbops.ColumnExists(
                        table_name=alter_table.name, column_name=col.name)

                    if prop.required:
                        # For some reason, Postgres allows dropping NOT NULL
                        # constraints from inherited columns, but we really
                        # should only always increase constraints down the
                        # inheritance chain.
                        cmd = dbops.AlterTableAlterColumnNull(
                            column_name=col.name, null=not prop.required)
                        alter_table.add_operation((cmd, (cond, ), None))

                    cmd = dbops.AlterTableAddColumn(col)
                    alter_table.add_operation((cmd, None, (cond, )))

        # Priority is set to 2 to make sure that INSERT is run after the host
        # link is INSERTed into edgedb.link.
        self.pgops.add(
            dbops.Insert(table=self.table, records=[rec], priority=2))

        return prop


class RenameProperty(
        PropertyMetaCommand, adapts=s_props.RenameProperty):
    def apply(self, schema, context=None):
        result = s_props.RenameProperty.apply(self, schema, context)
        PropertyMetaCommand.apply(self, schema, context)

        self.rename_pointer(
            result, schema, context, self.classname, self.new_name)

        return result


class AlterProperty(
        PropertyMetaCommand, adapts=s_props.AlterProperty):
    def apply(self, schema, context=None):
        metaclass = self.get_schema_metaclass()
        self.old_prop = old_prop = schema.get(
            self.classname, type=metaclass).copy()
        prop = s_props.AlterProperty.apply(self, schema, context)
        PropertyMetaCommand.apply(self, schema, context)

        with context(s_props.PropertyCommandContext(self, prop)):
            rec, updates = self.record_metadata(
                prop, old_prop, schema, context)
            self.updates = updates

            if rec:
                self.pgops.add(
                    dbops.Update(
                        table=self.table, record=rec, condition=[(
                            'name', str(prop.name))], priority=1))

            if isinstance(prop.target, s_scalars.ScalarType) and \
                    isinstance(self.old_prop.target, s_scalars.ScalarType) and\
                    prop.required != self.old_prop.required:

                src_ctx = context.get(s_links.LinkCommandContext)
                src_op = src_ctx.op
                alter_table = src_op.get_alter_table(context, priority=5)
                column_name = common.edgedb_name_to_pg_name(prop.shortname)
                if prop.required:
                    table = src_op._type_mech.get_table(src_ctx.scls, schema)
                    rec = table.record(**{column_name: dbops.Default()})
                    cond = [(column_name, None)]
                    update = dbops.Update(table, rec, cond, priority=4)
                    self.pgops.add(update)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnNull(
                        column_name=column_name, null=not prop.required))

            new_type = None
            for op in self.get_subcommands(type=sd.AlterObjectProperty):
                if (op.property == 'target' and
                        prop.shortname not in
                        {'std::source', 'std::target'}):
                    new_type = op.new_value.classname \
                        if op.new_value is not None else None
                    old_type = op.old_value.classname \
                        if op.old_value is not None else None
                    break

            if new_type:
                self.alter_host_table_column(
                    old_prop, prop, schema, context, old_type, new_type)

            self.alter_pointer_default(prop, schema, context)

        return prop


class DeleteProperty(
        PropertyMetaCommand, adapts=s_props.DeleteProperty):
    def apply(self, schema, context=None):
        property = s_props.DeleteProperty.apply(self, schema, context)
        PropertyMetaCommand.apply(self, schema, context)

        link = context.get(s_links.LinkCommandContext)

        if link:
            alter_table = link.op.get_alter_table(context)

            column_name = common.edgedb_name_to_pg_name(property.shortname)
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'

            col = dbops.AlterTableDropColumn(
                dbops.Column(name=column_name, type=column_type))
            alter_table.add_operation(col)

        self.pgops.add(
            dbops.Delete(
                table=self.table, condition=[('name', str(property.name))]))

        return property


class CreateMappingIndexes(MetaCommand):
    def __init__(self, table_name, cardinality, maplinks):
        super().__init__()

        key = str(table_name[1])
        if cardinality == s_pointers.PointerCardinality.OneToOne:
            # Each source can have only one target and
            # each target can have only one source
            sides = ('std::source', 'std::target')

        elif cardinality == s_pointers.PointerCardinality.OneToMany:
            # Each target can have only one source, but
            # one source can have many targets
            sides = ('std::target', )

        elif cardinality == s_pointers.PointerCardinality.ManyToOne:
            # Each source can have only one target, but
            # one target can have many sources
            sides = ('std::source', )

        else:
            sides = ()

        for side in sides:
            index = deltadbops.MappingIndex(
                key + '_%s' % side, cardinality, maplinks, table_name)
            index.add_columns((side, 'ptr_item_id'))
            self.pgops.add(dbops.CreateIndex(index, priority=3))


class AlterMappingIndexes(MetaCommand):
    def __init__(self, idx_names, table_name, cardinality, maplinks):
        super().__init__()

        self.pgops.add(DropMappingIndexes(idx_names, table_name, cardinality))
        self.pgops.add(CreateMappingIndexes(table_name, cardinality, maplinks))


class DropMappingIndexes(MetaCommand):
    def __init__(self, idx_names, table_name, cardinality):
        super().__init__()

        table_exists = dbops.TableExists(table_name)
        group = dbops.CommandGroup(conditions=(table_exists, ), priority=3)

        for idx_name in idx_names:
            idx = dbops.Index(name=idx_name, table_name=table_name)
            fq_idx_name = (table_name[0], idx_name)
            index_exists = dbops.IndexExists(fq_idx_name)
            drop = dbops.DropIndex(
                idx, conditions=(index_exists, ), priority=3)
            group.add_command(drop)

        self.pgops.add(group)


class UpdateMappingIndexes(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.links = {}
        self.idx_name_re = re.compile(
            r'.*(?P<cardinality>[1*]{2})_cardinality_idx$')
        self.idx_pred_re = re.compile(
            r'''
                              \( \s* ptr_item_id \s* = \s*
                                  (?:(?: ANY \s* \( \s* ARRAY \s* \[
                                      (?P<type_ids> \d+ (?:\s* , \s* \d+)* )
                                  \s* \] \s* \) \s* )
                                  |
                                  (?P<type_id>\d+))
                              \s* \)
                           ''', re.X)
        self.schema_exists = dbops.SchemaExists(name='edgedb')

    def interpret_index(self, index, link_map):
        index_name = index.name
        index_predicate = index.predicate
        m = self.idx_name_re.match(index_name)
        if not m:
            raise s_err.SchemaError(
                'could not interpret index %s' % index_name)

        cardinality = m.group('cardinality')

        m = self.idx_pred_re.match(index_predicate)
        if not m:
            raise s_err.SchemaError(
                'could not interpret index {} predicate: {}'.format(
                    (index_name, index_predicate)))

        ptr_item_ids = (
            int(i)
            for i in re.split(
                r'\D+', m.group('type_ids') or m.group('type_id')))

        links = []
        for i in ptr_item_ids:
            # XXX: in certain cases, orphaned indexes are left in the backend
            # after the link was dropped.
            try:
                links.append(link_map[i])
            except KeyError:
                pass

        return cardinality, links

    def interpret_indexes(self, table_name, indexes, link_map):
        for idx_data in indexes:
            idx = dbops.Index.from_introspection(table_name, idx_data)
            yield idx.name, self.interpret_index(idx, link_map)

    def _group_indexes(self, indexes):
        """Group indexes by link name."""
        for index_name, (cardinality, link_names) in indexes:
            for link_name in link_names:
                yield link_name, index_name

    def group_indexes(self, indexes):
        key = lambda i: i[0]
        grouped = itertools.groupby(
            sorted(self._group_indexes(indexes), key=key), key=key)
        for link_name, indexes in grouped:
            yield link_name, tuple(i[1] for i in indexes)

    async def apply(self, schema, context):
        db = context.db
        if await self.schema_exists.execute(context):
            link_map = await context._get_class_map(reverse=True)
            indexes = {}
            idx_data = await datasources.introspection.tables.fetch_indexes(
                db,
                schema_pattern='edgedb%', index_pattern='%_cardinality_idx')
            for row in idx_data:
                table_name = tuple(row['table_name'])
                indexes[table_name] = self.interpret_indexes(
                    table_name, row['indexes'], link_map)
        else:
            link_map = {}
            indexes = {}

        for link_name, ops in self.links.items():
            table_name = common.link_name_to_table_name(
                link_name, catenate=False)

            new_indexes = {
                k: []
                for k in s_pointers.PointerCardinality.__members__.values()
            }
            alter_indexes = {
                k: []
                for k in s_pointers.PointerCardinality.__members__.values()
            }

            existing = indexes.get(table_name)

            if existing:
                existing_by_name = dict(existing)
                existing = dict(self.group_indexes(existing_by_name.items()))
            else:
                existing_by_name = {}
                existing = {}

            processed = {}

            for op, scls in ops:
                already_processed = processed.get(scls.name)

                if isinstance(op, CreateLink):
                    # CreateLink can only happen once
                    if already_processed:
                        raise RuntimeError('duplicate CreateLink: {}'.format(
                            scls.name))

                    new_indexes[scls.cardinality].append(
                        (scls.name, None, None))

                elif isinstance(op, AlterLink):
                    # We are in apply stage, so the potential link changes,
                    # renames have not yet been pushed to the database, so
                    # link_map potentially contains old link names.
                    ex_idx_names = existing.get(op.old_link.name)

                    if ex_idx_names:
                        ex_idx = existing_by_name[ex_idx_names[0]]
                        queue = alter_indexes
                    else:
                        ex_idx = None
                        queue = new_indexes

                    item = (scls.name, op.old_link.name, ex_idx_names)

                    # Delta generator could have yielded several AlterLink
                    # commands for the same link, we need to respect only the
                    # last state.
                    if already_processed:
                        if already_processed != scls.cardinality:
                            queue[already_processed].remove(item)

                            if not ex_idx or ex_idx[0] != scls.cardinality:
                                queue[scls.cardinality].append(item)

                    elif not ex_idx or ex_idx[0] != scls.cardinality:
                        queue[scls.cardinality].append(item)

                processed[scls.name] = scls.cardinality

            for cardinality, maplinks in new_indexes.items():
                if maplinks:
                    maplinks = list(i[0] for i in maplinks)
                    self.pgops.add(
                        CreateMappingIndexes(
                            table_name, cardinality, maplinks))

            for cardinality, maplinks in alter_indexes.items():
                new = []
                alter = {}
                for maplink in maplinks:
                    maplink_name, orig_maplink_name, ex_idx_names = maplink
                    ex_idx = existing_by_name[ex_idx_names[0]]

                    alter_links = alter.get(ex_idx_names)
                    if alter_links is None:
                        alter[ex_idx_names] = alter_links = set(ex_idx[1])
                    alter_links.discard(orig_maplink_name)

                    new.append(maplink_name)

                if new:
                    self.pgops.add(
                        CreateMappingIndexes(table_name, cardinality, new))

                for idx_names, altlinks in alter.items():
                    if not altlinks:
                        self.pgops.add(
                            DropMappingIndexes(
                                ex_idx_names, table_name, cardinality))
                    else:
                        self.pgops.add(
                            AlterMappingIndexes(
                                idx_names, table_name, cardinality, altlinks))


class CommandContext(sd.CommandContext):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.class_name_to_id_map = None

    async def _get_class_map(self, reverse=False):
        classes = await datasources.schema.objects.fetch(self.db)
        grouped = itertools.groupby(classes, key=lambda i: i['id'])
        if reverse:
            class_map = {k: next(i)['name'] for k, i in grouped}
        else:
            class_map = {next(i)['name']: k for k, i in grouped}
        return class_map

    async def get_class_map(self):
        class_map = self.class_name_to_id_map
        if not class_map:
            class_map = await self._get_class_map()
            self.class_name_to_id_map = class_map
        return class_map


class ModuleMetaCommand(NamedObjectMetaCommand):
    table = metaschema.get_metaclass_table(s_mod.Module)


class CreateModule(ModuleMetaCommand, adapts=s_mod.CreateModule):
    def apply(self, schema, context):
        CompositeObjectMetaCommand.apply(self, schema, context)
        self.scls = module = s_mod.CreateModule.apply(self, schema, context)

        module_name = module.name
        schema_name = common.edgedb_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup(neg_conditions={condition})
        cmd.add_command(dbops.CreateSchema(name=schema_name))
        self.pgops.add(cmd)

        self.create_object(schema, module)

        return module


class AlterModule(ModuleMetaCommand, adapts=s_mod.AlterModule):
    def apply(self, schema, context):
        module = s_mod.AlterModule.apply(self, schema, context=context)
        CompositeObjectMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(module.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        return module


class DeleteModule(ModuleMetaCommand, adapts=s_mod.DeleteModule):
    def apply(self, schema, context):
        CompositeObjectMetaCommand.apply(self, schema, context)
        module = s_mod.DeleteModule.apply(self, schema, context)

        module_name = module.name
        schema_name = common.edgedb_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup()
        cmd.add_command(
            dbops.DropSchema(
                name=schema_name, conditions={condition}, priority=4))
        cmd.add_command(
            dbops.Delete(
                table=self.table, condition=[(
                    'name', str(module.name))]))

        self.pgops.add(cmd)

        return module


class CreateDatabase(ObjectMetaCommand, adapts=s_db.CreateDatabase):
    def apply(self, schema, context):
        s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.CreateDatabase(dbops.Database(self.name)))


class AlterDatabase(ObjectMetaCommand, adapts=s_db.AlterDatabase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}

    def apply(self, schema, context):
        self.update_mapping_indexes = UpdateMappingIndexes()

        s_db.AlterDatabase.apply(self, schema, context)
        MetaCommand.apply(self, schema)

        # self.update_mapping_indexes.apply(schema, context)
        self.pgops.add(self.update_mapping_indexes)

    def is_material(self):
        return True

    async def execute(self, context):
        for op in self.serialize_ops():
            await op.execute(context)

    def serialize_ops(self):
        queues = {}
        self._serialize_ops(self, queues)
        queues = (i[1] for i in sorted(queues.items(), key=lambda i: i[0]))
        return itertools.chain.from_iterable(queues)

    def _serialize_ops(self, obj, queues):
        for op in obj.pgops:
            if isinstance(op, MetaCommand):
                self._serialize_ops(op, queues)
            else:
                queue = queues.get(op.priority)
                if not queue:
                    queues[op.priority] = queue = []
                queue.append(op)


class DropDatabase(ObjectMetaCommand, adapts=s_db.DropDatabase):
    def apply(self, schema, context):
        s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.DropDatabase(self.name))
