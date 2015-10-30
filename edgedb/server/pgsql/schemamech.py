##
# Copyright (c) 2008-2015 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools
import re

from metamagic import caos
from metamagic.caos import proto
from metamagic.caos.ir import ast as irast
from metamagic.caos.ir import astexpr as irastexpr
from metamagic.caos.ir import utils as ir_utils
from metamagic.caos import caosql

from metamagic.utils import ast
from metamagic.utils import datastructures
from metamagic.utils import markup
from importkit import yaml
from importkit.import_ import get_object

from .datasources import introspection
from . import ast as pg_ast
from . import astexpr
from . import dbops
from . import deltadbops
from . import common
from . import types
from . import parser
from . import transformer
from . import codegen


class ConstraintMech:
    def __init__(self):
        self._constraints_cache = None

    def init_cache(self, connection):
        self._constraints_cache = self._populate_constraint_cache(connection)

    def invalidate_meta_cache(self):
        self._constraints_cache = None

    def _populate_constraint_cache(self, connection):
        constraints_ds = introspection.constraints.Constraints(connection)

        constraints = {}
        for row in constraints_ds.fetch(schema_pattern='caos%',
                                        constraint_pattern='%;schemaconstr%'):
            constraints[row['constraint_name']] = row

        return constraints

    def constraint_name_from_pg_name(self, connection, pg_name):
        if self._constraints_cache is None:
            self._constraints_cache = self._populate_constraint_cache(connection)

        try:
            cdata = self._constraints_cache[pg_name]
        except KeyError:
            return None
        else:
            name = cdata['constraint_description']
            name, _, _ = name.rpartition(';')
            return caos.name.Name(name)

    @classmethod
    def _get_unique_refs(cls, tree):
        # Check if the expression is not exists(<arg>) [and not exists (<arg>)...]
        expr = tree.selector[0].expr

        astexpr = irastexpr.ExistsConjunctionExpr()
        refs = astexpr.match(expr)

        if refs is None:
            return refs
        else:
            all_refs = []
            for ref in refs:
                # Unnest sequences in refs
                if (isinstance(ref, irast.BaseRefExpr)
                            and isinstance(ref.expr, irast.Sequence)):
                    all_refs.append(ref.expr)
                else:
                    all_refs.append(ref)

            return all_refs

    @classmethod
    def _get_ref_storage_info(cls, schema, refs):
        link_biased = {}
        concept_biased = {}

        ref_ptrs = {}
        for ref in refs:
            if isinstance(ref, irast.LinkPropRef):
                ptr = ref.ptr_proto
                src = ref.ref.link_proto
            elif isinstance(ref, irast.AtomicRef):
                ptr = ref.ptr_proto if ref.ptr_proto else ref.rlink.link_proto
                src = ref.ref
            elif isinstance(ref, irast.EntityLink):
                ptr = ref.link_proto
                src = ptr.source.concept if ptr.source else None
            elif isinstance(ref, irast.EntitySet):
                ptr = ref.rlink.link_proto
                src = ref.rlink.source.concept
            else:
                raise ValueError('unexpected ref type: {!r}'.format(ref))

            ref_ptrs[ref] = (ptr, src)

        for ref, (ptr, src) in ref_ptrs.items():
            ptr_info = types.get_pointer_storage_info(
                            ptr, source=src, resolve_type=False)

            # See if any of the refs are hosted in pointer tables and others are not...
            if ptr_info.table_type == 'link':
                link_biased[ref] = ptr_info
            else:
                concept_biased[ref] = ptr_info

            if link_biased and concept_biased:
                break

        if link_biased and concept_biased:
            for ref in concept_biased.copy():
                ptr, src = ref_ptrs[ref]
                ptr_info = types.get_pointer_storage_info(
                                ptr, source=src, resolve_type=False,
                                link_bias=True)

                if ptr_info.table_type == 'link':
                    link_biased[ref] = ptr_info
                    concept_biased.pop(ref)

        ref_tables = {}

        for ref, ptr_info in itertools.chain(concept_biased.items(), link_biased.items()):
            ptr, src = ref_ptrs[ref]

            try:
                ref_tables[ptr_info.table_name].append((ref, ptr, src, ptr_info))
            except KeyError:
                ref_tables[ptr_info.table_name] = [(ref, ptr, src, ptr_info)]

        return ref_tables

    @classmethod
    def _caosql_ref_to_pg_constr(cls, subject, tree, schema, link_bias):
        ircompiler = transformer.SimpleIRCompiler()
        sql_tree = ircompiler.transform(tree, protoschema=schema,
                                        local=True, link_bias=link_bias)

        is_multicol = isinstance(tree, irast.Sequence)

        # Determine if the sequence of references are all simple refs, not
        # expressions.  This influences the type of Postgres constraint used.
        #
        is_trivial = (isinstance(sql_tree, pg_ast.FieldRefNode)
                        or (isinstance(sql_tree, pg_ast.SequenceNode)
                            and all(isinstance(el, pg_ast.FieldRefNode)
                                    for el in sql_tree.elements)))

        # Find all field references
        #
        flt = lambda n: isinstance(n, pg_ast.FieldRefNode)
        refs = set(ast.find_children(sql_tree, flt))

        if isinstance(subject, proto.Atom):
            # Domain constraint, replace <atom_name> with VALUE

            subject_pg_name = common.caos_name_to_pg_name(subject.name)

            for ref in refs:
                if ref.field != subject_pg_name:
                    msg = 'unexpected node reference in Atom constraint: {}'.format(ref.field)
                    raise ValueError(msg)

                ref.field = 'VALUE'

        plain_expr = codegen.SQLSourceGenerator.to_source(sql_tree)

        if is_multicol:
            chunks = []

            for elem in sql_tree.elements:
                chunks.append(codegen.SQLSourceGenerator.to_source(elem))
        else:
            chunks = [plain_expr]

        if isinstance(sql_tree, pg_ast.FieldRefNode):
            refs.add(sql_tree)

        for ref in refs:
            ref.table = pg_ast.PseudoRelationNode(name="NEW", alias="NEW")
        new_expr = codegen.SQLSourceGenerator.to_source(sql_tree)

        for ref in refs:
            ref.table = pg_ast.PseudoRelationNode(name="OLD", alias="OLD")
        old_expr = codegen.SQLSourceGenerator.to_source(sql_tree)

        exprdata = dict(plain=plain_expr, plain_chunks=chunks, new=new_expr, old=old_expr)

        return dict(exprdata=exprdata, is_multicol=is_multicol, is_trivial=is_trivial)

    @classmethod
    def schema_constraint_to_backend_constraint(cls, subject, constraint, schema):
        assert constraint.subject is not None

        ir = caosql.compile_to_ir(constraint.finalexpr, schema,
                                  anchors={'subject': subject})

        terminal_refs = ir_utils.get_terminal_references(ir)

        ref_tables = cls._get_ref_storage_info(schema, terminal_refs)

        if len(ref_tables) > 1:
            raise ValueError('backend: multi-table constraints are not currently supported')
        elif ref_tables:
            subject_db_name = next(iter(ref_tables))
        else:
            subject_db_name = common.atom_name_to_domain_name(subject.name,
                                                              catenate=False)

        link_bias = ref_tables and next(iter(ref_tables.values()))[0][3].table_type == 'link'

        unique_expr_refs = cls._get_unique_refs(ir)

        pg_constr_data = {
            'subject_db_name': subject_db_name,
            'expressions': []
        }

        exprs = pg_constr_data['expressions']

        if unique_expr_refs:
            for ref in unique_expr_refs:
                exprdata = cls._caosql_ref_to_pg_constr(subject, ref, schema,
                                                        link_bias)
                exprs.append(exprdata)

            pg_constr_data['scope'] = 'relation'
            pg_constr_data['type'] = 'unique'
            pg_constr_data['subject_db_name'] = subject_db_name
        else:
            exprdata = cls._caosql_ref_to_pg_constr(subject, ir, schema,
                                                    link_bias)
            exprs.append(exprdata)

            pg_constr_data['subject_db_name'] = subject_db_name
            pg_constr_data['scope'] = 'row'
            pg_constr_data['type'] = 'check'

        if isinstance(constraint.subject, caos.types.ProtoAtom):
            constraint = SchemaDomainConstraint(subject=subject,
                                                constraint=constraint,
                                                pg_constr_data=pg_constr_data)
        else:
            constraint = SchemaTableConstraint(subject=subject,
                                               constraint=constraint,
                                               pg_constr_data=pg_constr_data)
        return constraint


class SchemaDomainConstraint:
    def __init__(self, subject, constraint, pg_constr_data):
        self._subject = subject
        self._constraint = constraint
        self._pg_constr_data = pg_constr_data

    @classmethod
    def _domain_constraint(cls, constr):
        domain_name = constr._pg_constr_data['subject_db_name']
        expressions = constr._pg_constr_data['expressions']

        constr = deltadbops.SchemaConstraintDomainConstraint(
                    domain_name, constr._constraint, expressions)

        return constr

    def create_ops(self):
        ops = dbops.CommandGroup()

        domconstr = self._domain_constraint(self)
        add_constr = dbops.AlterDomainAddConstraint(
                            name=domconstr.get_subject_name(quote=False),
                            constraint=domconstr)

        ops.add_command(add_constr)

        return ops

    def rename_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        domconstr = self._domain_constraint(self)
        orig_domconstr = self._domain_constraint(orig_constr)

        add_constr = dbops.AlterDomainRenameConstraint(
                            name=domconstr.get_subject_name(quote=False),
                            constraint=orig_domconstr,
                            new_constraint=domconstr)

        ops.add_command(add_constr)

        return ops

    def alter_ops(self, orig_constr):
        ops = dbops.CommandGroup()
        return ops

    def delete_ops(self):
        ops = dbops.CommandGroup()

        domconstr = self._domain_constraint(self)
        add_constr = dbops.AlterDomainDropConstraint(
                            name=domconstr.get_subject_name(quote=False),
                            constraint=domconstr)

        ops.add_command(add_constr)

        return ops


class SchemaTableConstraint:
    def __init__(self, subject, constraint, pg_constr_data):
        self._subject = subject
        self._constraint = constraint
        self._pg_constr_data = pg_constr_data

    @classmethod
    def _table_constraint(cls, constr):
        pg_c = constr._pg_constr_data

        table_name = pg_c['subject_db_name']
        expressions = pg_c['expressions']

        constr = deltadbops.SchemaConstraintTableConstraint(
                                    table_name,
                                    constraint=constr._constraint,
                                    exprdata=expressions,
                                    scope=pg_c['scope'],
                                    type=pg_c['type'])

        return constr

    def create_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        add_constr = deltadbops.AlterTableAddInheritableConstraint(
                                name=tabconstr.get_subject_name(quote=False),
                                constraint=tabconstr)

        ops.add_command(add_constr)

        return ops

    def rename_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        orig_tabconstr = self._table_constraint(orig_constr)

        rename_constr = deltadbops.AlterTableRenameInheritableConstraint(
                                name=tabconstr.get_subject_name(quote=False),
                                constraint=orig_tabconstr,
                                new_constraint=tabconstr)

        ops.add_command(rename_constr)

        return ops

    def alter_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        if self._constraint.is_abstract != orig_constr._constraint.is_abstract:
            tabconstr = self._table_constraint(self)
            orig_tabconstr = self._table_constraint(orig_constr)

            alter_constr = deltadbops.AlterTableAlterInheritableConstraint(
                                name=tabconstr.get_subject_name(quote=False),
                                constraint=orig_tabconstr,
                                new_constraint=tabconstr
                           )

            ops.add_command(alter_constr)

        return ops

    def delete_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        add_constr = deltadbops.AlterTableDropInheritableConstraint(
                                name=tabconstr.get_subject_name(quote=False),
                                constraint=tabconstr)

        ops.add_command(add_constr)

        return ops


class TypeMech:
    def __init__(self):
        self._column_cache = None
        self._table_cache = None

    def invalidate_meta_cache(self):
        self._column_cache = None
        self._table_cache = None

    def init_cache(self, connection):
        self._load_table_columns(('caos_%', None), connection)

    def _load_table_columns(self, table_name, connection):
        cols = introspection.tables.TableColumns(connection)
        cols = cols.fetch(table_name=table_name[1], schema_name=table_name[0])

        if self._column_cache is None:
            self._column_cache = {}

        for col in cols:
            try:
                table_cols = self._column_cache[(col['table_schema'], col['table_name'])]
            except KeyError:
                table_cols = collections.OrderedDict()
                self._column_cache[(col['table_schema'], col['table_name'])] = table_cols

            table_cols[col['column_name']] = col

    def get_table_columns(self, table_name, connection, cache='auto'):
        if cache is not None and self._column_cache is not None:
            cols = self._column_cache.get(table_name)
        else:
            cols = None

        if cols is None and cache != 'always':
            cols = self._load_table_columns(table_name, connection)

        return self._column_cache.get(table_name)

    def _load_type_attributes(self, type_name, connection):
        cols = introspection.types.CompositeTypeAttributes(connection)
        cols = cols.fetch(type_name=type_name[1], schema_name=type_name[0])

        if self._column_cache is None:
            self._column_cache = {}

        for col in cols:
            try:
                type_attrs = self._column_cache[(col['type_schema'], col['type_name'])]
            except KeyError:
                type_attrs = collections.OrderedDict()
                self._column_cache[(col['type_schema'], col['type_name'])] = type_attrs

            type_attrs[col['attribute_name']] = col

    def get_type_attributes(self, type_name, connection, cache='auto'):
        if cache is not None and self._column_cache is not None:
            cols = self._column_cache.get(type_name)
        else:
            cols = None

        if cols is None and cache != 'always':
            self._load_type_attributes(type_name, connection)

        return self._column_cache.get(type_name)

    def get_table(self, prototype, proto_schema):
        if self._table_cache is None:
            self._table_cache = {}

        table = self._table_cache.get(prototype)

        if table is None:
            table_name = common.get_table_name(prototype, catenate=False)
            table = dbops.Table(table_name)

            cols = []

            if isinstance(prototype, caos.types.ProtoLink):
                cols.extend([
                    dbops.Column(name='link_type_id', type='int'),
                    dbops.Column(name='metamagic.caos.builtins.linkid', type='uuid'),
                    dbops.Column(name='metamagic.caos.builtins.source', type='uuid'),
                    dbops.Column(name='metamagic.caos.builtins.target', type='uuid')
                ])

            elif isinstance(prototype, caos.types.ProtoConcept):
                cols.extend([
                    dbops.Column(name='concept_id', type='int')
                ])

            else:
                assert False

            if isinstance(prototype, proto.Concept):
                expected_table_type = 'concept'
            else:
                expected_table_type = 'link'

            for pointer_name, pointer in prototype.pointers.items():
                if not pointer.singular():
                    continue

                if pointer_name == 'metamagic.caos.builtins.source':
                    continue

                if pointer_name == 'metamagic.caos.builtins.linkid':
                    continue

                ptr_stor_info = types.get_pointer_storage_info(
                                    pointer, schema=proto_schema)

                if ptr_stor_info.column_name == 'metamagic.caos.builtins.target':
                    continue

                if ptr_stor_info.table_type == expected_table_type:
                    cols.append(dbops.Column(name=ptr_stor_info.column_name,
                                             type=ptr_stor_info.column_type))
            table.add_columns(cols)

            self._table_cache[prototype] = table

        return table
