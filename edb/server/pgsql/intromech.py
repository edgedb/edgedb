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

"""Low level instrospection of the schema."""


import collections
import importlib
import json
import pickle

from edb.lang.common import topological
from edb.lang.common import nlang

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
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import policy as s_policy
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

    def get_type_id(self, objtype):
        objtype_id = None

        type_cache = self.type_cache
        if type_cache:
            objtype_id = type_cache.get(objtype.name)

        if objtype_id is None:
            msg = 'could not determine backend id for type in this context'
            details = 'ObjectType: {}'.format(objtype.name)
            raise s_err.SchemaError(msg, details=details)

        return objtype_id

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

    async def readschema(self):
        schema = so.Schema()
        await self._init_introspection_cache()
        await self.read_modules(schema)
        await self.read_scalars(schema)
        await self.read_attributes(schema)
        await self.read_actions(schema)
        await self.read_events(schema)
        await self.read_objtypes(schema)
        await self.read_links(schema)
        await self.read_link_properties(schema)
        await self.read_policies(schema)
        await self.read_attribute_values(schema)
        await self.read_functions(schema)
        await self.read_constraints(schema)
        await self.read_indexes(schema)

        await self.order_attributes(schema)
        await self.order_actions(schema)
        await self.order_events(schema)
        await self.order_scalars(schema)
        await self.order_functions(schema)
        await self.order_link_properties(schema)
        await self.order_links(schema)
        await self.order_objtypes(schema)
        await self.order_policies(schema)

        return schema

    async def read_modules(self, schema):
        schemas = await introspection.schemas.fetch(
            self.connection, schema_pattern='edgedb_%')
        schemas = {
            s['name']
            for s in schemas if not s['name'].startswith('edgedb_aux_')
        }

        modules = await datasources.schema.modules.fetch(self.connection)
        modules = {
            common.edgedb_module_name_to_schema_name(m['name']):
            {'name': m['name'],
             'imports': m['imports']}
            for m in modules
        }

        recorded_schemas = set(modules.keys())

        # Sanity checks
        extra_schemas = schemas - recorded_schemas - {'edgedb', 'edgedbss'}
        missing_schemas = recorded_schemas - schemas

        if extra_schemas:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data schemas exist: {}'.format(
                ', '.join('"%s"' % s for s in extra_schemas))
            raise s_err.SchemaError(msg, details=details)

        if missing_schemas:
            msg = 'internal metadata incosistency'
            details = 'Missing schemas for modules: {}'.format(
                ', '.join('{!r}'.format(s) for s in missing_schemas))
            raise s_err.SchemaError(msg, details=details)

        mods = []

        for module in modules.values():
            mod = s_mod.Module(
                name=module['name'])
            schema.add_module(mod)
            mods.append(mod)

        for mod in mods:
            for imp_name in mod.imports:
                if not schema.has_module(imp_name):
                    # Must be a foreign module, import it directly
                    try:
                        impmod = importlib.import_module(imp_name)
                    except ImportError:
                        # Module has moved, create a dummy
                        impmod = so.DummyModule(imp_name)

                    schema.add_module(impmod)

    async def read_scalars(self, schema):
        seqs = await introspection.sequences.fetch(
            self.connection,
            schema_pattern='edgedb%', sequence_pattern='%_sequence')
        seqs = {(s['schema'], s['name']): s for s in seqs}

        seen_seqs = set()

        scalar_list = await datasources.schema.scalars.fetch(self.connection)

        basemap = {}

        for row in scalar_list:
            name = sn.Name(row['name'])

            scalar_data = {
                'name': name,
                'title': self.json_to_word_combination(row['title']),
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
                basemap[name] = scalar_data['bases']

            scalar = s_scalars.ScalarType(
                name=name, default=scalar_data['default'],
                title=scalar_data['title'],
                description=scalar_data['description'],
                is_abstract=scalar_data['is_abstract'],
                is_final=scalar_data['is_final'],
                view_type=scalar_data['view_type'],
                expr=scalar_data['expr'])

            schema.add(scalar)

        for scalar in schema.get_objects(type='ScalarType'):
            try:
                basename = basemap[scalar.name]
            except KeyError:
                pass
            else:
                scalar.bases = [schema.get(sn.Name(basename[0]))]

        sequence = schema.get('std::sequence', None)
        for scalar in schema.get_objects(type='ScalarType'):
            if (sequence is not None and scalar.issubclass(sequence) and
                    not scalar.is_abstract):
                seq_name = common.scalar_name_to_sequence_name(
                    scalar.name, catenate=False)
                if seq_name not in seqs:
                    msg = 'internal metadata incosistency'
                    details = (f'Missing sequence for sequence '
                               f'scalar {scalar.name}')
                    raise s_err.SchemaError(msg, details=details)
                seen_seqs.add(seq_name)

        extra_seqs = set(seqs) - seen_seqs
        if extra_seqs:
            msg = 'internal metadata incosistency'
            details = 'Extraneous sequences exist: {}'.format(
                ', '.join(common.qname(*t) for t in extra_seqs))
            raise s_err.SchemaError(msg, details=details)

    async def order_scalars(self, schema):
        for scalar in schema.get_objects(type='ScalarType'):
            scalar.acquire_ancestor_inheritance(schema)

    def _decode_func_params(self, row, schema):
        if row['params']:
            return [
                s_funcs.Parameter(
                    pos=r[0],
                    name=r[1],
                    default=r[2],
                    type=self.unpack_typeref(r[3], schema),
                    typemod=r[4],
                    kind=r[5])
                for r in row['params']
            ]
        else:
            return []

    async def read_functions(self, schema):
        func_list = await datasources.schema.functions.fetch(self.connection)

        for row in func_list:
            name = sn.Name(row['name'])

            func_data = {
                'name': name,
                'title': self.json_to_word_combination(row['title']),
                'description': row['description'],
                'aggregate': row['aggregate'],
                'params': self._decode_func_params(row, schema),
                'return_typemod': row['return_typemod'],
                'from_function': row['from_function'],
                'code': row['code'],
                'initial_value': row['initial_value'],
                'return_type': self.unpack_typeref(row['return_type'], schema)
            }

            func = s_funcs.Function(**func_data)
            schema.add(func)

    async def order_functions(self, schema):
        pass

    async def read_constraints(self, schema):
        constraints_list = await datasources.schema.constraints.fetch(
            self.connection)
        constraints_list = collections.OrderedDict((sn.Name(r['name']), r)
                                                   for r in constraints_list)

        basemap = {}

        for name, r in constraints_list.items():
            bases = tuple()

            if r['subject']:
                bases = (s_constr.Constraint.get_shortname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::constraint':
                bases = (sn.Name('std::constraint'), )

            title = self.json_to_word_combination(r['title'])
            description = r['description']
            subject = schema.get(r['subject']) if r['subject'] else None

            basemap[name] = bases

            constraint = s_constr.Constraint(
                name=name, subject=subject, title=title,
                params=self._decode_func_params(r, schema),
                description=description, is_abstract=r['is_abstract'],
                is_final=r['is_final'], expr=r['expr'],
                subjectexpr=r['subjectexpr'],
                localfinalexpr=r['localfinalexpr'], finalexpr=r['finalexpr'],
                errmessage=r['errmessage'],
                args=r['args'])

            if subject:
                subject.add_constraint(constraint)

            schema.add(constraint)

        for constraint in schema.get_objects(type='constraint'):
            try:
                bases = basemap[constraint.name]
            except KeyError:
                pass
            else:
                constraint.bases = [schema.get(b) for b in bases]

        for constraint in schema.get_objects(type='constraint'):
            constraint.acquire_ancestor_inheritance(schema)

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

            scls = coll_type.from_subtypes(st, typemods=typemods)

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

    async def read_indexes(self, schema):
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

        for index_data in await ds.fetch(self.connection):
            subj = schema.get(index_data['subject_name'])
            subj_table_name = common.get_table_name(subj, catenate=False)
            index_name = sn.Name(index_data['name'])

            try:
                pg_indexes.remove((subj_table_name, index_name))
            except KeyError:
                raise s_err.SchemaError(
                    'internal metadata inconsistency',
                    details=f'Index {index_name} is defined in schema, but'
                            f'the corresponding PostgreSQL index is missing.'
                ) from None

            index = s_indexes.SourceIndex(
                name=index_name,
                subject=subj,
                expr=index_data['expr'])

            subj.add_index(index)
            schema.add(index)

        if pg_indexes:
            details = f'Extraneous PostgreSQL indexes found: {pg_indexes!r}'
            raise s_err.SchemaError(
                'internal metadata inconsistency',
                details=details)

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
                    pointer.shortname, pointer.source.name)
            raise s_err.SchemaError(msg, details=details)

        return self._get_pointer_column_target(
            schema, pointer.source, pointer.shortname, col)

    def _get_pointer_column_target(self, schema, source, pointer_name, col):
        if col['column_type_schema'] == 'pg_catalog':
            col_type_schema = common.edgedb_module_name_to_schema_name('std')
            col_type = col['column_type']
        else:
            col_type_schema = col['column_type_schema']
            col_type = col['column_type']

        target = self.scalar_from_pg_type(col_type, col_type_schema, schema)
        return target, col['column_required']

    async def read_links(self, schema):
        link_tables = await introspection.tables.fetch_tables(
            self.connection,
            schema_pattern='edgedb%', table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        links_list = await datasources.schema.links.fetch(self.connection)
        links_list = collections.OrderedDict((sn.Name(r['name']), r)
                                             for r in links_list)

        basemap = {}

        for name, r in links_list.items():
            bases = tuple()

            if r['source']:
                bases = (s_links.Link.get_shortname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::link':
                bases = (sn.Name('std::link'), )

            if r['derived_from']:
                derived_from = schema.get(r['derived_from'])
            else:
                derived_from = None

            title = self.json_to_word_combination(r['title'])
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
                cardinality = s_pointers.PointerCardinality(r['cardinality'])
            else:
                cardinality = None

            basemap[name] = bases

            link = s_links.Link(
                name=name, source=source, target=target,
                spectargets=spectargets, cardinality=cardinality,
                required=required,
                title=title, description=description,
                derived_from=derived_from, is_derived=r['is_derived'],
                is_abstract=r['is_abstract'], is_final=r['is_final'],
                readonly=r['readonly'], computable=r['computable'],
                default=default)

            if spectargets:
                # Multiple specified targets,
                # target is a virtual derived object
                target = link.create_common_target(schema, spectargets)

            link_search = None

            if (isinstance(target, s_scalars.ScalarType) and
                    not source.is_derived):
                target, required = await self.read_pointer_target_column(
                    schema, link, None)

                objtype_schema, objtype_table = \
                    common.objtype_name_to_table_name(source.name,
                                                      catenate=False)

            link.target = target

            if link_search:
                link.search = link_search

            if source:
                source.add_pointer(link)

            schema.add(link)

        for link in schema.get_objects(type='link'):
            try:
                bases = basemap[link.name]
            except KeyError:
                pass
            else:
                link.bases = [schema.get(b) for b in bases]

        for link in schema.get_objects(type='link'):
            link.acquire_ancestor_inheritance(schema)

    async def order_links(self, schema):
        g = {}

        for link in schema.get_objects(type='link'):
            g[link.name] = {"item": link, "merge": [], "deps": []}
            if link.bases:
                g[link.name]['merge'].extend(b.name for b in link.bases)

        topological.normalize(g, merger=s_links.Link.merge, schema=schema)

        for link in schema.get_objects(type='link'):
            link.finalize(schema)

    async def read_link_properties(self, schema):
        link_props = await datasources.schema.links.fetch_properties(
            self.connection)
        link_props = collections.OrderedDict((sn.Name(r['name']), r)
                                             for r in link_props)
        basemap = {}

        for name, r in link_props.items():

            bases = ()

            if r['source']:
                bases = (s_props.Property.get_shortname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::property':
                bases = (sn.Name('std::property'), )

            title = self.json_to_word_combination(r['title'])
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
                cardinality = s_pointers.PointerCardinality(r['cardinality'])
            else:
                cardinality = None

            prop = s_props.Property(
                name=name, source=source, target=target, required=required,
                title=title, description=description, readonly=r['readonly'],
                computable=r['computable'], default=default,
                cardinality=cardinality, derived_from=derived_from,
                is_derived=r['is_derived'], is_abstract=r['is_abstract'],
                is_final=r['is_final'])

            if bases and bases[0] in {'std::target', 'std::source'}:
                if bases[0] == 'std::target' and source is not None:
                    target = source.target
                elif bases[0] == 'std::source' and source is not None:
                    target = source.source

            elif isinstance(target, s_scalars.ScalarType):
                target, required = \
                    await self.read_pointer_target_column(schema, prop, None)

            prop.target = target

            if source:
                prop.acquire_ancestor_inheritance(schema)
                source.add_pointer(prop)

            schema.add(prop)

        for prop in schema.get_objects(type='link_property'):
            try:
                bases = basemap[prop.name]
            except KeyError:
                pass
            else:
                prop.bases = [
                    schema.get(b, type=s_props.Property) for b in bases
                ]

    async def order_link_properties(self, schema):
        g = {}

        for prop in schema.get_objects(type='link_property'):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}
            if prop.bases:
                g[prop.name]['merge'].extend(b.name for b in prop.bases)

        topological.normalize(
            g, merger=s_props.Property.merge, schema=schema)

        for prop in schema.get_objects(type='link_property'):
            prop.finalize(schema)

    async def read_attributes(self, schema):
        attributes = await datasources.schema.attributes.fetch(self.connection)

        for r in attributes:
            name = sn.Name(r['name'])
            title = self.json_to_word_combination(r['title'])
            description = r['description']
            attribute = s_attrs.Attribute(
                name=name, title=title, description=description,
                type=self.unpack_typeref(r['type'], schema))
            schema.add(attribute)

    async def order_attributes(self, schema):
        pass

    async def read_attribute_values(self, schema):
        attributes = await datasources.schema.attributes.fetch_values(
            self.connection)

        for r in attributes:
            name = sn.Name(r['name'])
            subject = schema.get(r['subject_name'])
            attribute = schema.get(r['attribute_name'])
            value = pickle.loads(r['value'])

            attribute = s_attrs.AttributeValue(
                name=name, subject=subject, attribute=attribute, value=value)
            subject.add_attribute(attribute)
            schema.add(attribute)

    async def read_actions(self, schema):
        actions = await datasources.schema.policy.fetch_actions(
            self.connection)

        for r in actions:
            name = sn.Name(r['name'])
            title = self.json_to_word_combination(r['title'])
            description = r['description']

            action = s_policy.Action(
                name=name, title=title, description=description)
            schema.add(action)

    async def order_actions(self, schema):
        pass

    async def read_events(self, schema):
        events = await datasources.schema.policy.fetch_events(
            self.connection)

        basemap = {}

        for r in events:
            name = sn.Name(r['name'])
            title = self.json_to_word_combination(r['title'])
            description = r['description']

            if r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::event':
                bases = (sn.Name('std::event'), )
            else:
                bases = tuple()

            basemap[name] = bases

            event = s_policy.Event(
                name=name, title=title, description=description)
            schema.add(event)

        for event in schema.get_objects(type='event'):
            try:
                bases = basemap[event.name]
            except KeyError:
                pass
            else:
                event.bases = [schema.get(b) for b in bases]

        for event in schema.get_objects(type='event'):
            event.acquire_ancestor_inheritance(schema)

    async def order_events(self, schema):
        pass

    async def read_policies(self, schema):
        policies = await datasources.schema.policy.fetch_policies(
            self.connection)

        for r in policies:
            name = sn.Name(r['name'])
            title = self.json_to_word_combination(r['title'])
            description = r['description']
            policy = s_policy.Policy(
                name=name, title=title, description=description,
                subject=schema.get(r['subject']), event=schema.get(r['event']),
                actions=[schema.get(a) for a in r['actions']])
            schema.add(policy)
            policy.subject.add_policy(policy)

    async def order_policies(self, schema):
        pass

    async def get_type_attributes(self, type_name, connection=None,
                                  cache='auto'):
        return await self._type_mech.get_type_attributes(
            type_name, connection, cache)

    async def read_objtypes(self, schema):
        tables = await introspection.tables.fetch_tables(
            self.connection, schema_pattern='edgedb%', table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        objtype_list = await datasources.schema.objtypes.fetch(
            self.connection)
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
                'name': name,
                'title': self.json_to_word_combination(row['title']),
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

            objtype = s_objtypes.ObjectType(
                name=name, title=objtype['title'],
                description=objtype['description'],
                is_abstract=objtype['is_abstract'],
                is_final=objtype['is_final'],
                view_type=objtype['view_type'],
                expr=objtype['expr'])

            schema.add(objtype)

        for objtype in schema.get_objects(type='ObjectType'):
            try:
                bases = basemap[objtype.name]
            except KeyError:
                pass
            else:
                objtype.bases = [schema.get(b) for b in bases]

        derived = await datasources.schema.objtypes.fetch_derived(
            self.connection)

        for row in derived:
            attrs = dict(row)
            attrs['name'] = sn.SchemaName(attrs['name'])
            attrs['bases'] = [schema.get(b) for b in attrs['bases']]
            attrs['view_type'] = (s_types.ViewType(attrs['view_type'])
                                  if attrs['view_type'] else None)
            attrs['is_derived'] = True
            objtype = s_objtypes.ObjectType(**attrs)
            schema.add(objtype)

        tabdiff = set(tables.keys()) - visited_tables
        if tabdiff:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data tables exist: {}'.format(
                ', '.join('"%s.%s"' % t for t in tabdiff))
            raise s_err.SchemaError(msg, details=details)

    async def order_objtypes(self, schema):
        g = {}
        for objtype in schema.get_objects(type='ObjectType',
                                          include_derived=True):
            g[objtype.name] = {"item": objtype, "merge": [], "deps": []}
            if objtype.bases:
                g[objtype.name]["merge"].extend(b.name for b in objtype.bases)

        topological.normalize(
            g, merger=s_objtypes.ObjectType.merge, schema=schema)

        for objtype in schema.get_objects(type='ObjectType',
                                          include_derived=True):
            objtype.finalize(schema)

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
                scalar.acquire_ancestor_inheritance(schema)

        assert scalar
        return scalar

    def json_to_word_combination(self, data):
        if data:
            return nlang.WordCombination.from_dict(json.loads(data))
        else:
            return None

    async def translate_pg_error(self, query, error):
        return await errormech.ErrorMech._interpret_db_error(
            self, self._constr_mech, self._type_mech, error)
