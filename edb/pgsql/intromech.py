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


from edb import errors

from edb.common import topological

from edb import schema as so

from edb.schema import abc as s_abc
from edb.schema import attributes as s_attrs
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
from edb.schema import operators as s_opers
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
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
            schema = await self.read_modules(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_scalars(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_attributes(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_objtypes(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_casts(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_links(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_link_properties(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_operators(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_functions(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_constraints(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_indexes(
                schema, only_modules=modules, exclude_modules=exclude_modules)
            schema = await self.read_attribute_values(
                schema, only_modules=modules, exclude_modules=exclude_modules)

            schema = await self.order_scalars(schema)
            schema = await self.order_operators(schema)
            schema = await self.order_link_properties(schema)
            schema = await self.order_links(schema)
            schema = await self.order_objtypes(schema)

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
                'name': name,
                'is_abstract': row['is_abstract'],
                'is_final': row['is_final'],
                'view_type': (s_types.ViewType(row['view_type'])
                              if row['view_type'] else None),
                'bases': row['bases'],
                'default': (s_expr.Expression(**row['default'])
                            if row['default'] else None),
                'expr': (s_expr.Expression(**row['expr'])
                         if row['expr'] else None)
            }

            if scalar_data['bases']:
                basemap[name] = scalar_data.pop('bases')

            schema, scalar = s_scalars.ScalarType.create_in_schema(
                schema,
                **scalar_data
            )

        for scalar in schema.get_objects(type=s_scalars.ScalarType):
            try:
                basename = basemap[scalar.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = scalar.set_field_value(
                    schema, 'bases', [schema.get(sn.Name(basename[0]))])

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
        for scalar in schema.get_objects(type=s_scalars.ScalarType):
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
                    default=(s_expr.Expression(**param_data['default'])
                             if param_data['default'] else None),
                    type=self.unpack_typeref(param_data['type'], schema),
                    typemod=param_data['typemod'],
                    kind=param_data['kind'])
                params.append(param)

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

            oper_data = {
                'id': row['id'],
                'name': name,
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

            func_data = {
                'id': row['id'],
                'name': name,
                'language': row['language'],
                'params': params,
                'return_typemod': row['return_typemod'],
                'from_function': row['from_function'],
                'force_return_cast': row['force_return_cast'],
                'code': row['code'],
                'initial_value': row['initial_value'],
                'return_type': self.unpack_typeref(row['return_type'], schema)
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
            bases = tuple()

            if r['subject']:
                bases = (sn.shortname_from_fullname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::constraint':
                bases = (sn.Name('std::constraint'), )

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
                params=params,
                is_abstract=r['is_abstract'],
                is_final=r['is_final'],
                expr=s_expr.Expression(**r['expr']) if r['expr'] else None,
                subjectexpr=(s_expr.Expression(**r['subjectexpr'])
                             if r['subjectexpr'] else None),
                finalexpr=(s_expr.Expression(**r['finalexpr'])
                           if r['finalexpr'] else None),
                errmessage=r['errmessage'],
                args=([s_expr.Expression(**arg) for arg in r['args']]
                      if r['args'] is not None else None),
                return_type=self.unpack_typeref(r['return_type'], schema),
                return_typemod=r['return_typemod'],
            )

            if subject:
                schema = subject.add_constraint(schema, constraint)

        for constraint in schema.get_objects(type=s_constr.Constraint):
            try:
                bases = basemap[constraint.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = constraint.set_field_value(
                    schema, 'bases', [schema.get(b) for b in bases])

        for constraint in schema.get_objects(type=s_constr.Constraint):
            schema = constraint.acquire_ancestor_inheritance(schema)

        return schema

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
            scls = s_pseudo.Any.create()

        elif t['maintype'] == 'anytuple':
            scls = s_pseudo.AnyTuple.create()

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

    def interpret_indexes(self, table_name, indexes):
        for idx_data in indexes:
            yield dbops.Index.from_introspection(table_name, idx_data)

    async def read_indexes(self, schema, only_modules, exclude_modules):
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
        indexes = await ds.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        for index_data in indexes:
            subj = schema.get(index_data['subject_name'])
            subj_table_name = common.get_backend_name(
                schema, subj, catenate=False)
            index_name = sn.Name(index_data['name'])

            try:
                pg_indexes.remove((subj_table_name, index_name))
            except KeyError:
                raise errors.SchemaError(
                    'internal metadata inconsistency',
                    details=f'Index {index_name} is defined in schema, but'
                            f'the corresponding PostgreSQL index is missing.'
                ) from None

            schema, index = s_indexes.SourceIndex.create_in_schema(
                schema,
                id=index_data['id'],
                name=index_name,
                subject=subj,
                expr=s_expr.Expression(**index_data['expr']))

            schema = subj.add_index(schema, index)

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

        for name, r in links_list.items():
            bases = tuple()

            if r['source']:
                bases = (sn.shortname_from_fullname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::link':
                bases = (sn.Name('std::link'), )

            if r['derived_from']:
                derived_from = schema.get(r['derived_from'])
            else:
                derived_from = None

            source = schema.get(r['source']) if r['source'] else None
            if r['spectargets']:
                spectargets = [schema.get(t) for t in r['spectargets']]
                target = None
            else:
                spectargets = None
                target = self.unpack_typeref(r['target'], schema)

            if r['default']:
                default = s_expr.Expression(**r['default'])
            else:
                default = None

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

            schema = link.set_field_value(schema, 'target', target)

            if source:
                schema = source.add_pointer(schema, link)

        for link in schema.get_objects(type=s_links.Link):
            try:
                bases = basemap[link.get_name(schema)]
            except KeyError:
                pass
            else:
                schema = link.set_field_value(
                    schema,
                    'bases',
                    [schema.get(b) for b in bases])

        for link in schema.get_objects(type=s_links.Link):
            schema = link.acquire_ancestor_inheritance(schema)

        return schema

    async def order_links(self, schema):
        g = {}

        for link in schema.get_objects(type=s_links.Link):
            g[link.get_name(schema)] = {"item": link, "merge": [], "deps": []}
            link_bases = link.get_bases(schema).objects(schema)
            if link_bases:
                g[link.get_name(schema)]['merge'].extend(
                    b.get_name(schema) for b in link_bases)

        links = topological.sort(g)

        for link in links:
            schema = link.finalize(schema)

        return schema

    async def read_link_properties(
            self, schema, only_modules, exclude_modules):
        link_props = await datasources.schema.links.fetch_properties(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        link_props = {sn.Name(r['name']): r for r in link_props}
        basemap = {}

        for name, r in link_props.items():

            bases = ()

            if r['source']:
                bases = (sn.shortname_from_fullname(name), )
            elif r['bases']:
                bases = tuple(sn.Name(b) for b in r['bases'])
            elif name != 'std::property':
                bases = (sn.Name('std::property'), )

            source = schema.get(r['source']) if r['source'] else None

            if r['derived_from']:
                derived_from = schema.get(r['derived_from'])
            else:
                derived_from = None

            if r['default']:
                default = s_expr.Expression(**r['default'])
            else:
                default = None

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
                readonly=r['readonly'], computable=r['computable'],
                default=default, cardinality=cardinality,
                derived_from=derived_from, is_derived=r['is_derived'],
                is_abstract=r['is_abstract'], is_final=r['is_final'])

            if bases and bases[0] in {'std::target', 'std::source'}:
                if bases[0] == 'std::target' and source is not None:
                    target = source.get_target(schema)
                elif bases[0] == 'std::source' and source is not None:
                    target = source.get_source(schema)

            schema = prop.set_field_value(schema, 'target', target)

            if source:
                schema = prop.acquire_ancestor_inheritance(schema)
                schema = source.add_pointer(schema, prop)

        for prop in schema.get_objects(type=s_props.Property):
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

        for prop in schema.get_objects(type=s_props.Property):
            g[prop.get_name(schema)] = {"item": prop, "merge": [], "deps": []}
            prop_bases = prop.get_bases(schema).objects(schema)
            if prop_bases:
                g[prop.get_name(schema)]['merge'].extend(
                    b.get_name(schema) for b in prop_bases)

        props = topological.sort(g)

        for prop in props:
            schema = prop.finalize(schema)

        return schema

    async def read_attributes(self, schema, only_modules, exclude_modules):
        attributes = await datasources.schema.attributes.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

        for r in attributes:
            name = sn.Name(r['name'])
            schema, attribute = s_attrs.Attribute.create_in_schema(
                schema,
                id=r['id'],
                name=name,
                inheritable=r['inheritable'],
            )

        return schema

    async def read_attribute_values(
            self, schema, only_modules, exclude_modules):
        attributes = await datasources.schema.attributes.fetch_values(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)

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
                value=value,
                inheritable=r['inheritable'],
            )

            schema = subject.add_attribute(schema, attribute)

        return schema

    async def read_objtypes(self, schema, only_modules, exclude_modules):
        objtype_list = await datasources.schema.objtypes.fetch(
            self.connection, modules=only_modules,
            exclude_modules=exclude_modules)
        objtype_list = {sn.Name(row['name']): row for row in objtype_list}

        basemap = {}

        for name, row in objtype_list.items():
            objtype = {
                'id': row['id'],
                'name': name,
                'is_abstract': row['is_abstract'],
                'is_final': row['is_final'],
                'view_type': (s_types.ViewType(row['view_type'])
                              if row['view_type'] else None),
                'expr': (s_expr.Expression(**row['expr'])
                         if row['expr'] else None)
            }

            basemap[name] = row['bases'] or []

            schema, objtype = s_objtypes.ObjectType.create_in_schema(
                schema,
                id=objtype['id'],
                name=name,
                is_abstract=objtype['is_abstract'],
                is_final=objtype['is_final'],
                view_type=objtype['view_type'],
                expr=objtype['expr'])

        for objtype in schema.get_objects(type=s_objtypes.BaseObjectType):
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
            attrs['expr'] = (s_expr.Expression(**row['expr'])
                             if row['expr'] else None)
            attrs['is_derived'] = True
            schema, objtype = s_objtypes.ObjectType.create_in_schema(
                schema, **attrs)

        return schema

    async def order_objtypes(self, schema):
        g = {}
        for objtype in schema.get_objects(type=s_objtypes.BaseObjectType):
            g[objtype.get_name(schema)] = {
                "item": objtype, "merge": [], "deps": []
            }
            objtype_bases = objtype.get_bases(schema).objects(schema)
            if objtype_bases:
                g[objtype.get_name(schema)]["merge"].extend(
                    b.get_name(schema) for b in objtype_bases)

        objtypes = topological.sort(g)
        for objtype in objtypes:
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
