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


import collections
import json

from edb.lang.common import topological

from edb.lang import schema as so

from edb.lang.schema import attributes as s_attrs
from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import constraints as s_constr
from edb.lang.schema import error as s_err
from edb.lang.schema import expr as s_expr
from edb.lang.schema import functions as s_funcs
from edb.lang.schema import indexes as s_indexes
from edb.lang.schema import links as s_links
from edb.lang.schema import lproperties as s_props
from edb.lang.schema import modules as s_mod
from edb.lang.schema import name as sn
from edb.lang.schema import operators as s_opers
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import pseudo as s_pseudo
from edb.lang.schema import types as s_types

from edb.server.pgsql import common
from edb.server.pgsql import dbops

from . import datasources
from .datasources import introspection

from . import astexpr
from . import errormech
from . import parser
from . import schemamech
from . import types


class IntrospectionMech:

    def __init__(self, connection):
        self.schema = None
        self._constr_mech = schemamech.ConstraintMech()
        self._type_mech = schemamech.TypeMech()

        self.scalar_cache = {}
        self.link_cache = {}
        self.link_property_cache = {}
        self.type_cache = {}
        self.table_cache = {}
        self.domain_to_scalar_map = {}
        self.table_id_to_class_name_cache = {}
        self.classname_to_table_id_cache = {}
        self.attribute_link_map_cache = {}
        self._record_mapping_cache = {}

        self.parser = parser.PgSQLParser()
        self.search_idx_expr = astexpr.TextSearchExpr()
        self.type_expr = astexpr.TypeExpr()
        self.constant_expr = None

        self._operator_commutators = {}

        self.connection = connection

    def invalidate_cache(self):
        self.schema = None
        self._constr_mech.invalidate_schema_cache()
        self._type_mech.invalidate_schema_cache()
        self.link_cache.clear()
        self.link_property_cache.clear()
        self.type_cache.clear()
        self.scalar_cache.clear()
        self.table_cache.clear()
        self.domain_to_scalar_map.clear()
        self.table_id_to_class_name_cache.clear()
        self.classname_to_table_id_cache.clear()
        self.attribute_link_map_cache.clear()

    async def get_type_map(self, force_reload=False):
        if not self.type_cache or force_reload:
            cl_ds = datasources.schema.objtypes

            for row in await cl_ds.fetch(self.connection):
                self.type_cache[row['name']] = row['id']
                self.type_cache[row['id']] = sn.Name(row['name'])

            cl_ds = datasources.schema.scalars

            for row in await cl_ds.fetch(self.connection):
                self.type_cache[row['name']] = row['id']
                self.type_cache[row['id']] = sn.Name(row['name'])

        return self.type_cache

    async def _init_introspection_cache(self):
        await self._type_mech.init_cache(self.connection)
        await self._constr_mech.init_cache(self.connection)
        self.domain_to_scalar_map = await self._init_scalar_map_cache()
        # ObjectType map needed early for type filtering operations
        # in schema queries
        await self.get_type_map(force_reload=True)

    def table_name_to_object_name(self, table_name):
        return self.table_cache.get(table_name)['name']

    async def _init_scalar_map_cache(self):
        scalar_list = await datasources.schema.scalars.fetch(self.connection)

        domain_to_scalar_map = {}

        for row in scalar_list:
            name = sn.Name(row['name'])

            domain_name = common.scalar_name_to_domain_name(
                name, catenate=False)
            domain_to_scalar_map[domain_name] = name

        return domain_to_scalar_map

    async def getschema(self):
        if self.schema is None:
            self.schema = await self.readschema()

        return self.schema

    async def readschema(self, modules=None):
        await self._init_introspection_cache()

        schema = so.Schema()

        schema = await self.read_modules(schema, only_modules=modules)
        schema = await self.read_scalars(schema, only_modules=modules)
        schema = await self.read_attributes(schema, only_modules=modules)
        schema = await self.read_objtypes(schema, only_modules=modules)
        schema = await self.read_links(schema, only_modules=modules)
        schema = await self.read_link_properties(schema, only_modules=modules)
        schema = await self.read_attribute_values(schema, only_modules=modules)
        schema = await self.read_operators(schema, only_modules=modules)
        schema = await self.read_functions(schema, only_modules=modules)
        schema = await self.read_constraints(schema, only_modules=modules)
        schema = await self.read_indexes(schema, only_modules=modules)

        schema = await self.order_attributes(schema)
        schema = await self.order_scalars(schema)
        schema = await self.order_operators(schema)
        schema = await self.order_functions(schema)
        schema = await self.order_link_properties(schema)
        schema = await self.order_links(schema)
        schema = await self.order_objtypes(schema)

        return schema

    async def read_modules(self, schema, only_modules):
        schemas = await introspection.schemas.fetch(
            self.connection, schema_pattern='edgedb_%')
        schemas = {
            s['name']
            for s in schemas if not s['name'].startswith('edgedb_aux_')
        }

        modules = await datasources.schema.modules.fetch(
            self.connection, only_modules)

        modules = {
            common.edgedb_module_name_to_schema_name(m['name']):
            {'id': m['id'],
             'name': m['name']}
            for m in modules
        }

        recorded_schemas = set(modules.keys())

        # Sanity checks
        extra_schemas = schemas - recorded_schemas - {'edgedb', 'edgedbss'}
        missing_schemas = recorded_schemas - schemas

        if extra_schemas and not only_modules:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data schemas exist: {}'.format(
                ', '.join('"%s"' % s for s in extra_schemas))
            raise s_err.SchemaError(msg, details=details)

        if missing_schemas:
            msg = 'internal metadata incosistency'
            details = 'Missing schemas for modules: {}'.format(
                ', '.join('{!r}'.format(s) for s in missing_schemas))
            raise s_err.SchemaError(msg, details=details)

        for module in modules.values():
            schema, mod = s_mod.Module.create_in_schema(
                schema,
                name=module['name'])

        return schema

    async def read_scalars(self, schema, only_modules):
        seqs = await introspection.sequences.fetch(
            self.connection,
            schema_pattern='edgedb%', sequence_pattern='%_sequence')
        seqs = {(s['schema'], s['name']): s for s in seqs}

        seen_seqs = set()

        scalar_list = await datasources.schema.scalars.fetch(
            self.connection, only_modules)

        basemap = {}

        for row in scalar_list:
            name = sn.Name(row['name'])

            scalar_data = {
                'id': row['id'],
                'name': name,
                'title': row['title'],
                'description': row['description'],
                'is_abstract': row['is_abstract'],
                'is_final': row['is_final'],
                'view_type': (s_types.ViewType(row['view_type'])
                              if row['view_type'] else None),
                'bases': row['bases'],
                'default': row['default'],
                'expr': (s_expr.ExpressionText(row['expr'])
                         if row['expr'] else None)
            }

            self.scalar_cache[name] = scalar_data
            scalar_data['default'] = self.unpack_default(row['default'])

            if scalar_data['bases']:
                basemap[name] = scalar_data.pop('bases')

            schema, scalar = s_scalars.ScalarType.create_in_schema(
                schema,
                **scalar_data
            )

        for scalar in schema.get_objects(type='ScalarType'):
            try:
                basename = basemap[scalar.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = scalar.set_field_value(
                    schema, 'bases', [schema.get(sn.Name(basename[0]))])

        sequence = schema.get('std::sequence', None)
        for scalar in schema.get_objects(type='ScalarType'):
            if (sequence is not None and
                    scalar.issubclass(schema, sequence) and
                    not scalar.get_is_abstract(schema)):
                seq_name = common.scalar_name_to_sequence_name(
                    scalar.get_name(schema), catenate=False)
                if seq_name not in seqs:
                    msg = 'internal metadata incosistency'
                    details = (f'Missing sequence for sequence '
                               f'scalar {scalar.get_name(schema)}')
                    raise s_err.SchemaError(msg, details=details)
                seen_seqs.add(seq_name)

        extra_seqs = set(seqs) - seen_seqs
        if extra_seqs and not only_modules:
            msg = 'internal metadata incosistency'
            details = 'Extraneous sequences exist: {}'.format(
                ', '.join(common.qname(*t) for t in extra_seqs))
            raise s_err.SchemaError(msg, details=details)

        return schema

    async def order_scalars(self, schema):
        for scalar in schema.get_objects(type='ScalarType'):
            schema = scalar.acquire_ancestor_inheritance(schema)
        return schema

    def _decode_func_params(self, schema, row, param_map):
        if row['params']:
            params = []

            for r in row['params']:
                param_data = param_map.get(r)
                schema, param = s_funcs.Parameter.create_in_schema(
                    schema,
                    id=param_data['id'],
                    num=param_data['num'],
                    name=sn.Name(param_data['name']),
                    default=param_data['default'],
                    type=self.unpack_typeref(param_data['type'], schema),
                    typemod=param_data['typemod'],
                    kind=param_data['kind'])
                params.append(param)

            return schema, params
        else:
            return schema, []

    async def read_operators(self, schema, only_modules):
        self._operator_commutators.clear()

        ds = datasources.schema
        func_list = await ds.operators.fetch(
            self.connection, only_modules)
        param_list = await ds.functions.fetch_params(
            self.connection, only_modules)
        param_map = {p['name']: p for p in param_list}

        for row in func_list:
            name = sn.Name(row['name'])

            schema, params = self._decode_func_params(schema, row, param_map)

            oper_data = {
                'id': row['id'],
                'name': name,
                'operator_kind': row['operator_kind'],
                'title': row['title'],
                'description': row['description'],
                'language': row['language'],
                'params': params,
                'return_typemod': row['return_typemod'],
                'from_operator': row['from_operator'],
                'return_type': self.unpack_typeref(row['return_type'], schema)
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

    async def read_functions(self, schema, only_modules):
        ds = datasources.schema.functions
        func_list = await ds.fetch(self.connection, only_modules)
        param_list = await ds.fetch_params(self.connection, only_modules)
        param_map = {p['name']: p for p in param_list}

        for row in func_list:
            name = sn.Name(row['name'])

            schema, params = self._decode_func_params(schema, row, param_map)

            func_data = {
                'id': row['id'],
                'name': name,
                'title': row['title'],
                'description': row['description'],
                'language': row['language'],
                'params': params,
                'return_typemod': row['return_typemod'],
                'from_function': row['from_function'],
                'code': row['code'],
                'initial_value': row['initial_value'],
                'return_type': self.unpack_typeref(row['return_type'], schema)
            }

            schema, _ = s_funcs.Function.create_in_schema(schema, **func_data)

        return schema

    async def order_functions(self, schema):
        return schema

    async def read_constraints(self, schema, only_modules):
        ds = datasources.schema
        constraints_list = await ds.constraints.fetch(
            self.connection, modules=only_modules)
        constraints_list = collections.OrderedDict(
            (sn.Name(r['name']), r) for r in constraints_list)
        param_list = await ds.functions.fetch_params(
            self.connection, modules=only_modules)
        param_map = {p['name']: p for p in param_list}

        basemap = {}

        for name, r in constraints_list.items():
            bases = tuple()

            if r['subject']:
                bases = (s_constr.Constraint.shortname_from_fullname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::constraint':
                bases = (sn.Name('std::constraint'), )

            title = r['title']
            description = r['description']
            subject = schema.get(r['subject']) if r['subject'] else None

            basemap[name] = r['bases'] or []

            if not r['subject']:
                schema, params = self._decode_func_params(schema, r, param_map)
            else:
                params = None

            schema, constraint = s_constr.Constraint.create_in_schema(
                schema,
                id=r['id'],
                name=name,
                subject=subject,
                title=title,
                params=params,
                description=description, is_abstract=r['is_abstract'],
                is_final=r['is_final'], expr=r['expr'],
                subjectexpr=r['subjectexpr'],
                finalexpr=r['finalexpr'],
                errmessage=r['errmessage'],
                args=r['args'],
                return_type=self.unpack_typeref(r['return_type'], schema),
                return_typemod=r['return_typemod'],
            )

            if subject:
                schema = subject.add_constraint(schema, constraint)

        for constraint in schema.get_objects(type='constraint'):
            try:
                bases = basemap[constraint.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = constraint.set_field_value(
                    schema, 'bases', [schema.get(b) for b in bases])

        for constraint in schema.get_objects(type='constraint'):
            schema = constraint.acquire_ancestor_inheritance(schema)

        return schema

    async def order_constraints(self, schema):
        pass

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
            if issubclass(coll_type, s_types.Tuple) and named:
                st = collections.OrderedDict(subtypes)
                typemods = {'named': named}
            else:
                st = [st[1] for st in subtypes]

            scls = coll_type.from_subtypes(schema, st, typemods=typemods)

        elif t['maintype'] == 'anytype':
            scls = s_pseudo.Any.create()

        else:
            scls = schema.get(t['maintype'])

        return t['name'], scls

    def unpack_typedesc_nodes(self, types, schema):
        result = []
        if types:
            typemap = {
                t['id']: t for t in types
            }

            for t in types:
                if t['is_root']:
                    node = self._unpack_typedesc_node(typemap, t['id'], schema)
                    result.append(node)

        return result

    def unpack_typeref(self, typedesc, schema):
        if typedesc:
            result = self.unpack_typedesc_nodes(typedesc['types'], schema)
            if result:
                return result[0][1]

    def unpack_default(self, value):
        result = None
        if value is not None:
            val = json.loads(value)
            if val['type'] == 'expr':
                result = s_expr.ExpressionText(val['value'])
            else:
                result = val['value']
        return result

    def interpret_indexes(self, table_name, indexes):
        for idx_data in indexes:
            yield dbops.Index.from_introspection(table_name, idx_data)

    async def read_indexes(self, schema, only_modules):
        pg_index_data = await introspection.tables.fetch_indexes(
            self.connection,
            schema_pattern='edgedb%', index_pattern='%_reg_idx')

        pg_indexes = set()
        for row in pg_index_data:
            table_name = tuple(row['table_name'])
            for pg_index in self.interpret_indexes(table_name, row['indexes']):
                pg_indexes.add(
                    (table_name, pg_index.get_metadata('schemaname'))
                )

        ds = datasources.schema.indexes
        indexes = await ds.fetch(self.connection, modules=only_modules)

        for index_data in indexes:
            subj = schema.get(index_data['subject_name'])
            subj_table_name = common.get_table_name(
                schema, subj, catenate=False)
            index_name = sn.Name(index_data['name'])

            try:
                pg_indexes.remove((subj_table_name, index_name))
            except KeyError:
                raise s_err.SchemaError(
                    'internal metadata inconsistency',
                    details=f'Index {index_name} is defined in schema, but'
                            f'the corresponding PostgreSQL index is missing.'
                ) from None

            schema, index = s_indexes.SourceIndex.create_in_schema(
                schema,
                id=index_data['id'],
                name=index_name,
                subject=subj,
                expr=index_data['expr'])

            schema = subj.add_index(schema, index)

        if pg_indexes and not only_modules:
            details = f'Extraneous PostgreSQL indexes found: {pg_indexes!r}'
            raise s_err.SchemaError(
                'internal metadata inconsistency',
                details=details)

        return schema

    async def read_pointer_target_column(self, schema, pointer,
                                         constraints_cache):
        ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=schema, resolve_type=False)
        cols = await self._type_mech.get_table_columns(
            ptr_stor_info.table_name, connection=self.connection)

        col = cols.get(ptr_stor_info.column_name)

        if not col:
            msg = 'internal metadata inconsistency'
            details = (
                'Record for {!r} hosted by {!r} exists, but ' +
                'the corresponding table column is missing').format(
                    pointer.get_shortname(schema),
                    pointer.get_source(schema).get_name(schema))
            raise s_err.SchemaError(msg, details=details)

        return self._get_pointer_column_target(
            schema,
            pointer.get_source(schema),
            pointer.get_shortname(schema),
            col)

    def _get_pointer_column_target(self, schema, source, pointer_name, col):
        if col['column_type_schema'] == 'pg_catalog':
            col_type_schema = common.edgedb_module_name_to_schema_name('std')
            col_type = col['column_type']
        else:
            col_type_schema = col['column_type_schema']
            col_type = col['column_type']

        target = self.scalar_from_pg_type(col_type, col_type_schema, schema)
        return target, col['column_required']

    async def read_links(self, schema, only_modules):
        link_tables = await introspection.tables.fetch_tables(
            self.connection,
            schema_pattern='edgedb%', table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        links_list = await datasources.schema.links.fetch(
            self.connection, modules=only_modules)
        links_list = collections.OrderedDict((sn.Name(r['name']), r)
                                             for r in links_list)

        basemap = {}

        for name, r in links_list.items():
            bases = tuple()

            if r['source']:
                bases = (s_links.Link.shortname_from_fullname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::link':
                bases = (sn.Name('std::link'), )

            if r['derived_from']:
                derived_from = schema.get(r['derived_from'])
            else:
                derived_from = None

            title = r['title']
            description = r['description']

            source = schema.get(r['source']) if r['source'] else None
            if r['spectargets']:
                spectargets = [schema.get(t) for t in r['spectargets']]
                target = None
            else:
                spectargets = None
                target = self.unpack_typeref(r['target'], schema)

            default = self.unpack_default(r['default'])

            required = r['required']

            if r['cardinality']:
                cardinality = s_pointers.Cardinality(r['cardinality'])
            else:
                cardinality = None

            basemap[name] = bases

            schema, link = s_links.Link.create_in_schema(
                schema,
                id=r['id'],
                name=name,
                source=source,
                target=target,
                spectargets=spectargets,
                cardinality=cardinality,
                required=required,
                title=title,
                description=description,
                derived_from=derived_from,
                is_derived=r['is_derived'],
                is_abstract=r['is_abstract'],
                is_final=r['is_final'],
                readonly=r['readonly'],
                computable=r['computable'],
                default=default)

            if spectargets:
                # Multiple specified targets,
                # target is a virtual derived object
                schema, target = link.create_common_target(schema, spectargets)

            if (isinstance(target, s_scalars.ScalarType) and
                    not source.get_is_derived(schema)):
                target, required = await self.read_pointer_target_column(
                    schema, link, None)

                objtype_schema, objtype_table = \
                    common.objtype_name_to_table_name(source.get_name(schema),
                                                      catenate=False)

            schema = link.set_field_value(schema, 'target', target)

            if source:
                schema = source.add_pointer(schema, link)

        for link in schema.get_objects(type='link'):
            try:
                bases = basemap[link.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = link.set_field_value(
                    schema,
                    'bases',
                    [schema.get(b) for b in bases])

        for link in schema.get_objects(type='link'):
            schema = link.acquire_ancestor_inheritance(schema)

        return schema

    async def order_links(self, schema):
        g = {}

        for link in schema.get_objects(type='link'):
            g[link.get_name(schema)] = {"item": link, "merge": [], "deps": []}
            link_bases = link.get_bases(schema).objects(schema)
            if link_bases:
                g[link.get_name(schema)]['merge'].extend(
                    b.get_name(schema) for b in link_bases)

        topological.normalize(g, merger=s_links.Link.merge, schema=schema)

        for link in schema.get_objects(type='link'):
            schema = link.finalize(schema)

        return schema

    async def read_link_properties(self, schema, only_modules):
        link_props = await datasources.schema.links.fetch_properties(
            self.connection, modules=only_modules)
        link_props = collections.OrderedDict((sn.Name(r['name']), r)
                                             for r in link_props)
        basemap = {}

        for name, r in link_props.items():

            bases = ()

            if r['source']:
                bases = (s_props.Property.shortname_from_fullname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::property':
                bases = (sn.Name('std::property'), )

            title = r['title']
            description = r['description']
            source = schema.get(r['source']) if r['source'] else None

            if r['derived_from']:
                derived_from = schema.get(r['derived_from'])
            else:
                derived_from = None

            default = self.unpack_default(r['default'])

            required = r['required']
            target = self.unpack_typeref(r['target'], schema)
            basemap[name] = bases

            if r['cardinality']:
                cardinality = s_pointers.Cardinality(r['cardinality'])
            else:
                cardinality = None

            schema, prop = s_props.Property.create_in_schema(
                schema,
                id=r['id'],
                name=name, source=source, target=target, required=required,
                title=title, description=description, readonly=r['readonly'],
                computable=r['computable'], default=default,
                cardinality=cardinality, derived_from=derived_from,
                is_derived=r['is_derived'], is_abstract=r['is_abstract'],
                is_final=r['is_final'])

            if bases and bases[0] in {'std::target', 'std::source'}:
                if bases[0] == 'std::target' and source is not None:
                    target = source.get_target(schema)
                elif bases[0] == 'std::source' and source is not None:
                    target = source.get_source(schema)

            elif isinstance(target, s_scalars.ScalarType):
                target, required = \
                    await self.read_pointer_target_column(schema, prop, None)

            schema = prop.set_field_value(schema, 'target', target)

            if source:
                schema = prop.acquire_ancestor_inheritance(schema)
                schema = source.add_pointer(schema, prop)

        for prop in schema.get_objects(type='link_property'):
            try:
                bases = basemap[prop.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = prop.set_field_value(
                    schema,
                    'bases',
                    [schema.get(b, type=s_props.Property) for b in bases])

        return schema

    async def order_link_properties(self, schema):
        g = {}

        for prop in schema.get_objects(type='link_property'):
            g[prop.get_name(schema)] = {"item": prop, "merge": [], "deps": []}
            prop_bases = prop.get_bases(schema).objects(schema)
            if prop_bases:
                g[prop.get_name(schema)]['merge'].extend(
                    b.get_name(schema) for b in prop_bases)

        topological.normalize(
            g, merger=s_props.Property.merge, schema=schema)

        for prop in schema.get_objects(type='link_property'):
            schema = prop.finalize(schema)

        return schema

    async def read_attributes(self, schema, only_modules):
        attributes = await datasources.schema.attributes.fetch(
            self.connection, modules=only_modules)

        for r in attributes:
            name = sn.Name(r['name'])
            title = r['title']
            description = r['description']
            schema, attribute = s_attrs.Attribute.create_in_schema(
                schema,
                id=r['id'],
                name=name, title=title, description=description,
                type=self.unpack_typeref(r['type'], schema))

        return schema

    async def order_attributes(self, schema):
        return schema

    async def read_attribute_values(self, schema, only_modules):
        attributes = await datasources.schema.attributes.fetch_values(
            self.connection, modules=only_modules)

        for r in attributes:
            name = sn.Name(r['name'])
            subject = schema.get(r['subject_name'])
            attribute = schema.get(r['attribute_name'])
            value = r['value']

            schema, attribute = s_attrs.AttributeValue.create_in_schema(
                schema,
                id=r['id'],
                name=name,
                subject=subject,
                attribute=attribute,
                value=value)

            schema = subject.add_attribute(schema, attribute)

        return schema

    async def get_type_attributes(self, type_name, connection=None,
                                  cache='auto'):
        return await self._type_mech.get_type_attributes(
            type_name, connection, cache)

    async def read_objtypes(self, schema, only_modules):
        tables = await introspection.tables.fetch_tables(
            self.connection, schema_pattern='edgedb%', table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        objtype_list = await datasources.schema.objtypes.fetch(
            self.connection, modules=only_modules)
        objtype_list = collections.OrderedDict((sn.Name(row['name']), row)
                                               for row in objtype_list)

        visited_tables = set()

        self.table_cache.update({
            common.objtype_name_to_table_name(n, catenate=False): c
            for n, c in objtype_list.items()
        })

        basemap = {}

        for name, row in objtype_list.items():
            objtype = {
                'id': row['id'],
                'name': name,
                'title': row['title'],
                'description': row['description'],
                'is_abstract': row['is_abstract'],
                'is_final': row['is_final'],
                'view_type': (s_types.ViewType(row['view_type'])
                              if row['view_type'] else None),
                'expr': (s_expr.ExpressionText(row['expr'])
                         if row['expr'] else None)
            }

            table_name = common.objtype_name_to_table_name(
                name, catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for type {!r} exists but ' \
                          'the table is missing'.format(name)
                raise s_err.SchemaError(msg, details=details)

            visited_tables.add(table_name)

            bases = await self.pg_table_inheritance_to_bases(
                table['name'], table['schema'], self.table_cache)

            basemap[name] = bases

            schema, objtype = s_objtypes.ObjectType.create_in_schema(
                schema,
                id=objtype['id'],
                name=name, title=objtype['title'],
                description=objtype['description'],
                is_abstract=objtype['is_abstract'],
                is_final=objtype['is_final'],
                view_type=objtype['view_type'],
                expr=objtype['expr'])

        for objtype in schema.get_objects(type='ObjectType'):
            try:
                bases = basemap[objtype.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = objtype.set_field_value(
                    schema, 'bases', [schema.get(b) for b in bases])

        derived = await datasources.schema.objtypes.fetch_derived(
            self.connection)

        for row in derived:
            attrs = dict(row)
            attrs['name'] = sn.SchemaName(attrs['name'])
            attrs['bases'] = [schema.get(b) for b in attrs['bases']]
            attrs['view_type'] = (s_types.ViewType(attrs['view_type'])
                                  if attrs['view_type'] else None)
            attrs['is_derived'] = True
            schema, objtype = s_objtypes.ObjectType.create_in_schema(
                schema, **attrs)

        tabdiff = set(tables.keys()) - visited_tables
        if tabdiff and not only_modules:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data tables exist: {}'.format(
                ', '.join('"%s.%s"' % t for t in tabdiff))
            raise s_err.SchemaError(msg, details=details)

        return schema

    async def order_objtypes(self, schema):
        g = {}
        for objtype in schema.get_objects(type='ObjectType',
                                          include_derived=True):
            g[objtype.get_name(schema)] = {
                "item": objtype, "merge": [], "deps": []
            }
            objtype_bases = objtype.get_bases(schema).objects(schema)
            if objtype_bases:
                g[objtype.get_name(schema)]["merge"].extend(
                    b.get_name(schema) for b in objtype_bases)

        topological.normalize(
            g, merger=s_objtypes.ObjectType.merge, schema=schema)

        for objtype in schema.get_objects(type='ObjectType',
                                          include_derived=True):
            schema = objtype.finalize(schema)

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

    def parse_pg_type(self, type_expr):
        tree = self.parser.parse('None::' + type_expr)
        typname, typmods = self.type_expr.match(tree)
        return typname, typmods

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

    def scalar_from_pg_type(self, type_expr, scalar_schema, schema):
        typname = (type_expr,)
        typmods = None
        domain_name = typname[-1]
        scalar_name = self.domain_to_scalar_map.get(
            (scalar_schema, domain_name))

        if scalar_name:
            scalar = schema.get(scalar_name, None)
        else:
            scalar = None

        if not scalar:

            typeconv = self.pg_type_to_scalar_name_and_constraints(
                typname, typmods)
            if typeconv:
                name, _ = typeconv
                scalar = schema.get(name)

        assert scalar
        return scalar

    async def translate_pg_error(self, query, error):
        return await errormech.ErrorMech._interpret_db_error(
            self, self._constr_mech, self._type_mech, error)
