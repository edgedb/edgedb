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

"""Low level introspection of the schema."""

from __future__ import annotations

import json

import immutables as immu

from edb import errors

from edb.edgeql import qltypes

from edb import schema as so
from edb.schema import abc as s_abc
from edb.schema import annos as s_anno
from edb.schema import casts as s_casts
from edb.schema import scalars as s_scalars
from edb.schema import objtypes as s_objtypes
from edb.schema import constraints as s_constr
from edb.schema import expr as s_expr
from edb.schema import functions as s_funcs
from edb.schema import indexes as s_indexes
from edb.schema import links as s_links
from edb.schema import lproperties as s_props
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import operators as s_opers
from edb.schema import pseudo as s_pseudo
from edb.schema import roles as s_roles
from edb.schema import types as s_types

from edb.pgsql import common
from edb.pgsql import dbops

from . import datasources
from .datasources import introspection

from . import schemamech
from . import types


class IntrospectionMech:

    def __init__(self, connection):
        self._constr_mech = schemamech.ConstraintMech()

        self._operator_commutators = {}

        self.connection = connection

    async def readschema(self, *, schema=None, modules=None,
                         exclude_modules=None):
        if schema is None:
            schema = so.Schema()

        async with self.connection.transaction(isolation='repeatable_read'):
            schema = await self.read_roles(
                schema)
            schema = await self.read_modules(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_scalars(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_annotations(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema, obj_exprmap = await self.read_objtypes(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_casts(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema, link_exprmap = await self.read_links(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema, prop_exprmap = await self.read_link_properties(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_operators(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_functions(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_constraints(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_indexes(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_annotation_values(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_views(
                schema, only_modules=modules, exclude_modules=exclude_modules)

            schema = await self.order_scalars(schema)
            schema = await self.order_operators(schema)
            schema = await self.order_link_properties(schema, prop_exprmap)
            schema = await self.order_links(schema, link_exprmap)
            schema = await self.order_objtypes(schema, obj_exprmap)

        return schema

    async def read_roles(self, schema):
        roles = await datasources.schema.roles.fetch(self.connection)
        basemap = {}

        for row in roles:
            schema, role = s_roles.Role.create_in_schema(
                schema,
                id=row['id'],
                name=row['name'],
                is_superuser=row['is_superuser'],
                allow_login=row['allow_login'],
                password=row['password'],
            )

            basemap[role.id] = tuple(row['bases']) if row['bases'] else ()

        for role in schema.get_objects(type=s_roles.Role):
            try:
                bases = basemap[role.id]
            except KeyError:
                pass
            else:
                schema = role.set_field_value(
                    schema,
                    'bases',
                    [schema.get_by_id(b) for b in bases])

        return schema

    async def read_modules(self, schema, only_modules, exclude_modules):
        schemas = await introspection.schemas.fetch(
            self.connection, schema_pattern='edgedb_%')
        schemas = {
            s['name']
            for s in schemas if not s['name'].startswith('edgedb_aux_')
        }

        modules = await datasources.schema.modules.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        modules = [
            {'id': m['id'], 'name': m['name']}
            for m in modules
        ]

        recorded_schemas = set()
        for module in modules:
            schema, mod = s_mod.Module.create_in_schema(
                schema,
                id=module['id'],
                name=module['name'])

            recorded_schemas.add(common.get_backend_name(schema, mod))

        # Sanity checks
        extra_schemas = schemas - recorded_schemas - {'edgedb', 'edgedbss'}
        missing_schemas = recorded_schemas - schemas

        if extra_schemas and not only_modules and not exclude_modules:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data schemas exist: {}'.format(
                ', '.join('"%s"' % s for s in extra_schemas))
            raise errors.SchemaError(msg, details=details)

        if missing_schemas:
            msg = 'internal metadata incosistency'
            details = 'Missing schemas for modules: {}'.format(
                ', '.join('{!r}'.format(s) for s in missing_schemas))
            raise errors.SchemaError(msg, details=details)

        return schema

    async def read_scalars(self, schema, only_modules, exclude_modules):
        seqs = await introspection.sequences.fetch(
            self.connection,
            schema_pattern='edgedb%', sequence_pattern='%_sequence')
        seqs = {(s['schema'], s['name']): s for s in seqs}

        seen_seqs = set()

        scalar_list = await datasources.schema.scalars.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        basemap = {}

        for row in scalar_list:
            name = sn.Name(row['name'])

            scalar_data = {
                'id': row['id'],
                'inherited_fields': self._unpack_inherited_fields(
                    row['inherited_fields']),
                'name': name,
                'is_abstract': row['is_abstract'],
                'is_final': row['is_final'],
                'view_type': (s_types.ViewType(row['view_type'])
                              if row['view_type'] else None),
                'view_is_persistent': row['view_is_persistent'],
                'default': (self.unpack_expr(row['default'], schema)
                            if row['default'] else None),
                'expr': (self.unpack_expr(row['expr'], schema)
                         if row['expr'] else None),
                'enum_values': row['enum_values'],
                'backend_id': row['backend_id'],
            }

            schema, scalar = s_scalars.ScalarType.create_in_schema(
                schema,
                **scalar_data
            )

            basemap[scalar] = (row['bases'], row['ancestors'])

        for scls, (basenames, ancestors) in basemap.items():
            schema = self._set_reflist(schema, scls, 'bases', basenames)
            schema = self._set_reflist(schema, scls, 'ancestors', ancestors)

        sequence = schema.get('std::sequence', None)
        for scalar in schema.get_objects(type=s_scalars.ScalarType):
            if (sequence is not None and
                    scalar.issubclass(schema, sequence) and
                    not scalar.get_is_abstract(schema)):
                seq_name = common.get_backend_name(
                    schema, scalar, catenate=False, aspect='sequence')
                if seq_name not in seqs:
                    msg = 'internal metadata incosistency'
                    details = (f'Missing sequence for sequence '
                               f'scalar {scalar.get_name(schema)}')
                    raise errors.SchemaError(msg, details=details)
                seen_seqs.add(seq_name)

        extra_seqs = set(seqs) - seen_seqs
        if extra_seqs and not only_modules and not exclude_modules:
            msg = 'internal metadata incosistency'
            details = 'Extraneous sequences exist: {}'.format(
                ', '.join(common.qname(*t) for t in extra_seqs))
            raise errors.SchemaError(msg, details=details)

        return schema

    async def order_scalars(self, schema):
        return schema

    def _set_reflist(self, schema, scls, field, namelist):
        if namelist is None:
            objs = []
        else:
            objs = [schema.get(sn.Name(name)) for name in namelist]
        return scls.set_field_value(schema, field, objs)

    def _decode_func_params(self, schema, row, param_map):
        if row['params']:
            params = []

            for r in row['params']:
                param_data = param_map.get(r)
                param = schema.get(r, None)
                if param is None:
                    if param_data['default']:
                        default = self.unpack_expr(
                            param_data['default'], schema)
                    else:
                        default = None
                    schema, param = s_funcs.Parameter.create_in_schema(
                        schema,
                        id=param_data['id'],
                        num=param_data['num'],
                        name=sn.Name(param_data['name']),
                        default=default,
                        type=self.unpack_typeref(param_data['type'], schema),
                        typemod=param_data['typemod'],
                        kind=param_data['kind'])
                params.append(param)

                p_type = param.get_type(schema)
                if p_type.is_collection():
                    schema, _ = p_type.as_schema_coll(schema)

            return schema, params
        else:
            return schema, []

    async def read_operators(self, schema, only_modules, exclude_modules):
        self._operator_commutators.clear()

        ds = datasources.schema
        func_list = await ds.operators.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        param_list = await ds.functions.fetch_params(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        param_map = {p['name']: p for p in param_list}

        for row in func_list:
            name = sn.Name(row['name'])

            schema, params = self._decode_func_params(schema, row, param_map)

            r_type = self.unpack_typeref(row['return_type'], schema)
            if r_type.is_collection():
                schema, _ = r_type.as_schema_coll(schema)

            oper_data = {
                'id': row['id'],
                'name': name,
                'is_abstract': row['is_abstract'],
                'operator_kind': row['operator_kind'],
                'language': row['language'],
                'params': params,
                'return_typemod': row['return_typemod'],
                'from_operator': row['from_operator'],
                'from_function': row['from_function'],
                'from_expr': row['from_expr'],
                'force_return_cast': row['force_return_cast'],
                'code': row['code'],
                'recursive': row['recursive'],
                'volatility': row['volatility'],
                'return_type': r_type,
            }

            schema, oper = s_opers.Operator.create_in_schema(
                schema, **oper_data)

            if row['commutator']:
                self._operator_commutators[oper] = row['commutator']

        return schema

    async def order_operators(self, schema):
        for oper, commutator in self._operator_commutators.items():
            schema = oper.set_field_value(
                schema, 'commutator', schema.get(commutator))

        self._operator_commutators.clear()
        return schema

    async def read_casts(self, schema, only_modules, exclude_modules):
        self._operator_commutators.clear()

        ds = datasources.schema
        cast_list = await ds.casts.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        for row in cast_list:
            name = sn.Name(row['name'])

            cast_data = {
                'id': row['id'],
                'name': name,
                'from_type': self.unpack_typeref(row['from_type'], schema),
                'to_type': self.unpack_typeref(row['to_type'], schema),
                'language': row['language'],
                'from_cast': row['from_cast'],
                'from_function': row['from_function'],
                'from_expr': row['from_expr'],
                'allow_implicit': row['allow_implicit'],
                'allow_assignment': row['allow_assignment'],
                'code': row['code'],
                'volatility': row['volatility'],
            }

            schema, oper = s_casts.Cast.create_in_schema(
                schema, **cast_data)

        return schema

    async def read_functions(self, schema, only_modules, exclude_modules):
        ds = datasources.schema.functions
        func_list = await ds.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        param_list = await ds.fetch_params(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        param_map = {p['name']: p for p in param_list}

        for row in func_list:
            name = sn.Name(row['name'])

            schema, params = self._decode_func_params(schema, row, param_map)

            r_type = self.unpack_typeref(row['return_type'], schema)
            if r_type.is_collection():
                schema, _ = r_type.as_schema_coll(schema)

            if row['initial_value']:
                initial_value = self.unpack_expr(row['initial_value'], schema)
            else:
                initial_value = None

            func_data = {
                'id': row['id'],
                'name': name,
                'language': row['language'],
                'params': params,
                'return_typemod': row['return_typemod'],
                'from_function': row['from_function'],
                'from_expr': row['from_expr'],
                'force_return_cast': row['force_return_cast'],
                'sql_func_has_out_params': row['sql_func_has_out_params'],
                'error_on_null_result': row['error_on_null_result'],
                'code': row['code'],
                'initial_value': initial_value,
                'session_only': row['session_only'],
                'volatility': row['volatility'],
                'return_type': r_type,
            }

            schema, _ = s_funcs.Function.create_in_schema(schema, **func_data)

        return schema

    async def read_constraints(self, schema, only_modules, exclude_modules):
        ds = datasources.schema
        constraints_list = await ds.constraints.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        constraints_list = {sn.Name(r['name']): r for r in constraints_list}
        param_list = await ds.functions.fetch_params(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        param_map = {p['name']: p for p in param_list}

        basemap = {}

        for name, r in constraints_list.items():
            subject = schema.get(r['subject']) if r['subject'] else None

            schema, params = self._decode_func_params(schema, r, param_map)

            schema, constraint = s_constr.Constraint.create_in_schema(
                schema,
                id=r['id'],
                inherited_fields=self._unpack_inherited_fields(
                    r['inherited_fields']),
                name=name,
                subject=subject,
                params=params,
                is_abstract=r['is_abstract'],
                is_final=r['is_final'],
                is_local=r['is_local'],
                delegated=r['delegated'],
                expr=(self.unpack_expr(r['expr'], schema)
                      if r['expr'] else None),
                subjectexpr=(self.unpack_expr(r['subjectexpr'], schema)
                             if r['subjectexpr'] else None),
                orig_subjectexpr=r['orig_subjectexpr'],
                finalexpr=(self.unpack_expr(r['finalexpr'], schema)
                           if r['finalexpr'] else None),
                errmessage=r['errmessage'],
                args=([self.unpack_expr(arg, schema) for arg in r['args']]
                      if r['args'] is not None else None),
                return_type=self.unpack_typeref(r['return_type'], schema),
                return_typemod=r['return_typemod'],
            )

            basemap[constraint] = (r['bases'], r['ancestors'])

            if subject:
                schema = subject.add_constraint(schema, constraint)

        for scls, (basenames, ancestors) in basemap.items():
            schema = self._set_reflist(schema, scls, 'bases', basenames)
            schema = self._set_reflist(schema, scls, 'ancestors', ancestors)

        return schema

    def _unpack_inherited_fields(self, value):
        if value is None:
            return immu.Map()
        else:
            return immu.Map(json.loads(value))

    def _unpack_typedesc_node(self, typemap, id, schema):
        t = typemap[id]

        if t['collection'] is not None:
            coll_type = s_types.Collection.get_class(t['collection'])
            subtypes = [
                self._unpack_typedesc_node(typemap, stid, schema)
                for stid in t['subtypes']
            ]
            if t['dimensions']:
                typemods = (t['dimensions'],)
            else:
                typemods = None

            named = all(s[0] is not None for s in subtypes)
            if issubclass(coll_type, s_abc.Tuple) and named:
                st = dict(subtypes)
                typemods = {'named': named}
            else:
                st = [st[1] for st in subtypes]

            scls = coll_type.from_subtypes(schema, st, typemods=typemods)

        elif t['maintype'] == 'anytype':
            scls = s_pseudo.Any.instance()

        elif t['maintype'] == 'anytuple':
            scls = s_pseudo.AnyTuple.instance()

        else:
            type_id = t['maintype']
            if type_id == s_obj.get_known_type_id('anytype'):
                scls = s_pseudo.Any.instance()
            elif type_id == s_obj.get_known_type_id('anytuple'):
                scls = s_pseudo.AnyTuple.instance()
            else:
                scls = schema.get_by_id(t['maintype'])

        return t['name'], scls

    def unpack_typedesc_nodes(self, types, schema):
        result = []
        if types:
            typemap = {
                t['id']: t for t in types
            }

            for t in types:
                if t['position'] is None:
                    node = self._unpack_typedesc_node(typemap, t['id'], schema)
                    result.append(node)

        return result

    def unpack_typeref(self, typedesc, schema):
        if typedesc:
            result = self.unpack_typedesc_nodes(typedesc['types'], schema)
            if result:
                return result[0][1]

    def unpack_expr(self, expr, schema):
        text, origtext, refs = expr

        if refs is None:
            refs = []

        refs = s_obj.ObjectSet.create(
            schema, [schema.get_by_id(ref) for ref in refs])

        return s_expr.Expression(text=text, origtext=origtext, refs=refs)

    def interpret_indexes(self, table_name, indexes):
        for idx_data in indexes:
            yield dbops.Index.from_introspection(table_name, idx_data)

    async def read_indexes(self, schema, only_modules, exclude_modules):
        pg_index_data = await introspection.tables.fetch_indexes(
            self.connection,
            schema_pattern='edgedb%', index_pattern='%_index')

        pg_indexes = set()
        for row in pg_index_data:
            table_name = tuple(row['table_name'])
            for pg_index in self.interpret_indexes(table_name, row['indexes']):
                pg_indexes.add(
                    (table_name, pg_index.get_metadata('schemaname'))
                )

        ds = datasources.schema.indexes
        indexes = await ds.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        basemap = {}

        for index_data in indexes:
            subj = schema.get(index_data['subject_name'])
            subj_table_name = common.get_backend_name(
                schema, subj, catenate=False)
            index_name = sn.Name(index_data['name'])

            if index_data['is_local']:
                try:
                    pg_indexes.remove((subj_table_name, index_name))
                except KeyError:
                    raise errors.SchemaError(
                        'internal metadata inconsistency',
                        details=(
                            f'Index {index_name} is defined in schema, but '
                            f'the corresponding PostgreSQL index is missing.'
                        )
                    ) from None

            schema, index = s_indexes.Index.create_in_schema(
                schema,
                id=index_data['id'],
                name=index_name,
                subject=subj,
                is_local=index_data['is_local'],
                origexpr=index_data['origexpr'],
                inherited_fields=self._unpack_inherited_fields(
                    index_data['inherited_fields']),
                expr=self.unpack_expr(index_data['expr'], schema))

            schema = subj.add_index(schema, index)

            basemap[index] = (index_data['bases'], index_data['ancestors'])

        for scls, (basenames, ancestors) in basemap.items():
            schema = self._set_reflist(schema, scls, 'bases', basenames)
            schema = self._set_reflist(schema, scls, 'ancestors', ancestors)

        if pg_indexes and not only_modules and not exclude_modules:
            details = f'Extraneous PostgreSQL indexes found: {pg_indexes!r}'
            raise errors.SchemaError(
                'internal metadata inconsistency',
                details=details)

        return schema

    async def read_links(self, schema, only_modules, exclude_modules):
        link_tables = await introspection.tables.fetch_tables(
            self.connection,
            schema_pattern='edgedb%', table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        links_list = await datasources.schema.links.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        links_list = {sn.Name(r['name']): r for r in links_list}

        basemap = {}
        exprmap = {}

        for name, r in links_list.items():
            bases = tuple()

            if r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::link':
                bases = (sn.Name('std::link'), )

            source = schema.get(r['source']) if r['source'] else None
            target = self.unpack_typeref(r['target'], schema)

            required = r['required']

            if r['cardinality']:
                cardinality = qltypes.Cardinality(r['cardinality'])
            else:
                cardinality = None

            schema, link = s_links.Link.create_in_schema(
                schema,
                id=r['id'],
                inherited_fields=self._unpack_inherited_fields(
                    r['inherited_fields']),
                name=name,
                source=source,
                target=target,
                cardinality=cardinality,
                required=required,
                is_derived=r['is_derived'],
                is_abstract=r['is_abstract'],
                is_final=r['is_final'],
                is_local=r['is_local'],
                readonly=r['readonly'],
                expr=(self.unpack_expr(r['expr'], schema)
                      if r['expr'] else None),
            )

            if r['default']:
                exprmap[link] = r['default']

            basemap[link] = (bases, r['ancestors'])

            schema = link.set_field_value(schema, 'target', target)

            if source:
                schema = source.add_pointer(schema, link)

        for scls, (basenames, ancestors) in basemap.items():
            schema = self._set_reflist(schema, scls, 'bases', basenames)
            schema = self._set_reflist(schema, scls, 'ancestors', ancestors)

        return schema, exprmap

    async def order_links(self, schema, exprmap):
        for link, deflt in exprmap.items():
            default = self.unpack_expr(deflt, schema)
            schema = link.set_field_value(schema, 'default', default)

        return schema

    async def read_link_properties(
            self, schema, only_modules, exclude_modules):
        link_props = await datasources.schema.links.fetch_properties(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        link_props = {sn.Name(r['name']): r for r in link_props}
        basemap = {}
        exprmap = {}

        for name, r in link_props.items():

            bases = ()

            if r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::property':
                bases = (sn.Name('std::property'), )

            source = schema.get(r['source']) if r['source'] else None

            required = r['required']
            target = self.unpack_typeref(r['target'], schema)

            if target is not None and target.is_collection():
                schema, _ = target.as_schema_coll(schema)

            if r['cardinality']:
                cardinality = qltypes.Cardinality(r['cardinality'])
            else:
                cardinality = None

            schema, prop = s_props.Property.create_in_schema(
                schema,
                id=r['id'],
                inherited_fields=self._unpack_inherited_fields(
                    r['inherited_fields']),
                name=name, source=source, target=target, required=required,
                readonly=r['readonly'],
                expr=(self.unpack_expr(r['expr'], schema)
                      if r['expr'] else None),
                cardinality=cardinality,
                is_derived=r['is_derived'],
                is_local=r['is_local'],
                is_abstract=r['is_abstract'], is_final=r['is_final'])

            basemap[prop] = (bases, r['ancestors'])

            if r['default']:
                exprmap[prop] = r['default']

            if bases and bases[0] in {'std::target', 'std::source'}:
                if bases[0] == 'std::target' and source is not None:
                    target = source.get_target(schema)
                elif bases[0] == 'std::source' and source is not None:
                    target = source.get_source(schema)

            schema = prop.set_field_value(schema, 'target', target)

            if source:
                schema = source.add_pointer(schema, prop)

        for scls, (basenames, ancestors) in basemap.items():
            schema = self._set_reflist(schema, scls, 'bases', basenames)
            schema = self._set_reflist(schema, scls, 'ancestors', ancestors)

        return schema, exprmap

    async def order_link_properties(self, schema, exprmap):
        for prop, deflt in exprmap.items():
            default = self.unpack_expr(deflt, schema)
            schema = prop.set_field_value(schema, 'default', default)

        return schema

    async def read_annotations(self, schema, only_modules, exclude_modules):
        annotations = await datasources.schema.annos.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        for r in annotations:
            name = sn.Name(r['name'])
            schema, _ = s_anno.Annotation.create_in_schema(
                schema,
                id=r['id'],
                inherited_fields=self._unpack_inherited_fields(
                    r['inherited_fields']),
                name=name,
                inheritable=r['inheritable'],
            )

        return schema

    async def read_annotation_values(
            self, schema, only_modules, exclude_modules):
        annotations = await datasources.schema.annos.fetch_values(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        basemap = {}

        for r in annotations:
            name = sn.Name(r['name'])
            subject = schema.get(r['subject_name'])
            anno = schema.get(r['annotation_name'])
            value = r['value']

            schema, anno = s_anno.AnnotationValue.create_in_schema(
                schema,
                id=r['id'],
                inherited_fields=self._unpack_inherited_fields(
                    r['inherited_fields']),
                name=name,
                subject=subject,
                annotation=anno,
                value=value,
                inheritable=r['inheritable'],
                is_local=r['is_local'],
            )

            basemap[anno] = (r['bases'], r['ancestors'])

            schema = subject.add_annotation(schema, anno)

        for scls, (basenames, ancestors) in basemap.items():
            schema = self._set_reflist(schema, scls, 'bases', basenames)
            schema = self._set_reflist(schema, scls, 'ancestors', ancestors)

        return schema

    async def read_objtypes(self, schema, only_modules, exclude_modules):
        objtype_list = await datasources.schema.objtypes.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        objtype_list = {sn.Name(row['name']): row for row in objtype_list}

        basemap = {}
        exprmap = {}

        for name, row in objtype_list.items():
            objtype = {
                'id': row['id'],
                'inherited_fields': self._unpack_inherited_fields(
                    row['inherited_fields']),
                'name': name,
                'is_abstract': row['is_abstract'],
                'is_final': row['is_final'],
                'view_type': (s_types.ViewType(row['view_type'])
                              if row['view_type'] else None),
                'view_is_persistent': row['view_is_persistent'],
            }

            exprmap[name] = row['expr']

            if row['union_of']:
                union_of = [schema.get(t) for t in row['union_of']]
            else:
                union_of = None

            schema, objtype = s_objtypes.ObjectType.create_in_schema(
                schema,
                id=objtype['id'],
                name=name,
                is_abstract=objtype['is_abstract'],
                union_of=union_of,
                is_final=objtype['is_final'],
                view_type=objtype['view_type'],
                view_is_persistent=objtype['view_is_persistent'],
            )

            basemap[objtype] = (row['bases'], row['ancestors'])

        for scls, (basenames, ancestors) in basemap.items():
            schema = self._set_reflist(schema, scls, 'bases', basenames)
            schema = self._set_reflist(schema, scls, 'ancestors', ancestors)

        return schema, exprmap

    async def order_objtypes(self, schema, exprmap):
        for objtype in schema.get_objects(type=s_objtypes.BaseObjectType):
            try:
                expr = exprmap[objtype.get_name(schema)]
            except KeyError:
                pass
            else:
                if expr is not None:
                    schema = objtype.set_field_value(
                        schema, 'expr', self.unpack_expr(expr, schema))

        return schema

    async def read_views(self, schema, only_modules, exclude_modules):
        tuple_views = await datasources.schema.types.fetch_tuple_views(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        for r in tuple_views:
            eltypes = self.unpack_typeref(r['element_types'], schema)

            schema, tview = s_types.TupleView.create_in_schema(
                schema,
                id=r['id'],
                name=sn.Name(r['name']),
                view_type=s_types.ViewType(r['view_type']),
                view_is_persistent=r['view_is_persistent'],
                named=r['named'],
                expr=self.unpack_expr(r['expr'], schema),
                element_types=s_obj.ObjectDict.create(
                    schema, dict(eltypes.iter_subtypes(schema))),
            )

        array_views = await datasources.schema.types.fetch_array_views(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        for r in array_views:
            eltype = self.unpack_typeref(r['element_type'], schema)

            schema, tview = s_types.ArrayView.create_in_schema(
                schema,
                id=r['id'],
                name=sn.Name(r['name']),
                view_type=s_types.ViewType(r['view_type']),
                view_is_persistent=r['view_is_persistent'],
                expr=self.unpack_expr(r['expr'], schema),
                element_type=eltype,
                dimensions=r['dimensions'],
            )

        return schema

    async def pg_table_inheritance(self, table_name, schema_name):
        inheritance = await introspection.tables.fetch_inheritance(
            self.connection,
            table_pattern=table_name, schema_pattern=schema_name, max_depth=1)
        return tuple(i[:2] for i in inheritance[1:])

    async def pg_table_inheritance_to_bases(
            self, table_name, schema_name, table_to_objtype_map):
        bases = []

        for table in await self.pg_table_inheritance(table_name, schema_name):
            base = table_to_objtype_map[tuple(table[:2])]
            bases.append(base['name'])

        return tuple(bases)

    def pg_type_to_scalar_name_and_constraints(self, typname, typmods):
        if len(typname) > 1 and typname[0] != 'pg_catalog':
            return None
        else:
            typname = typname[-1]

        typeconv = types.base_type_name_map_r.get(typname)
        if typeconv:
            if isinstance(typeconv, sn.Name):
                name = typeconv
                constraints = ()
            else:
                name, constraints = typeconv(
                    self.connection, typname, *typmods)
            return name, constraints
        return None
