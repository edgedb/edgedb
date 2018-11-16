#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


"""Facility for loading EdgeDB module declarations into a schema."""

import collections
import itertools
import typing

from edb.lang.common import ast
from edb.lang.common import ordered
from edb.lang.common import topological

from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import codegen as qlcodegen
from edb.lang.edgeql import functypes as ft
from edb.lang.edgeql import utils as qlutils

from edb.lang.edgeql.parser.grammar import lexutils as ql_lexutils

from edb.lang.ir import ast as ir_ast
from edb.lang.ir import inference as ir_inference
from edb.lang.ir import utils as ir_utils

from . import ast as s_ast
from . import parser as s_parser

from . import attributes as s_attrs
from . import delta as s_delta
from . import objtypes as s_objtypes
from . import constraints as s_constr
from . import error as s_err
from . import expr as s_expr
from . import functions as s_func
from . import indexes as s_indexes
from . import links as s_links
from . import lproperties as s_props
from . import modules as s_mod
from . import name as s_name
from . import objects as s_obj
from . import pseudo as s_pseudo
from . import scalars as s_scalars
from . import schema as s_schema
from . import types as s_types
from . import utils as s_utils


_DECL_MAP = {
    s_ast.ScalarTypeDeclaration: s_scalars.ScalarType,
    s_ast.ObjectTypeDeclaration: s_objtypes.ObjectType,
    s_ast.ConstraintDeclaration: s_constr.Constraint,
    s_ast.LinkDeclaration: s_links.Link,
    s_ast.AttributeDeclaration: s_attrs.Attribute,
}


class DeclarationLoader:
    def __init__(self, schema):
        self._schema = schema
        self._mod_aliases = {}

    def load_module(self, module_name, decl_ast):
        decls = decl_ast.declarations

        self._module = module = s_mod.Module(name=module_name)
        self._schema = self._schema.add_module(module)
        self._mod_aliases[None] = module_name

        self._process_imports(decl_ast)

        order = s_schema.Schema.global_dep_order
        objects = collections.OrderedDict(
            (t, collections.OrderedDict()) for t in order)

        # First, iterate over all top-level declarations
        # to get a sense of what's in the schema so that
        # forward references work.
        for decl in decls:
            try:
                objcls = _DECL_MAP[type(decl)]
            except KeyError:
                if isinstance(decl, s_ast.Import):
                    continue
                msg = 'unexpected declaration type: {!r}'.format(decl)
                raise TypeError(msg) from None

            name = s_name.Name(module=module_name, name=decl.name)

            # Only populate the absolute minimum necessary for
            # the next steps.  _setdefaults_ and _relaxrequired_  set
            # to False instruct the object to skip validation for now.

            # TODO: refactor this
            objcls_kw = {}
            if hasattr(decl, 'abstract'):
                objcls_kw['is_abstract'] = decl.abstract
            if hasattr(decl, 'delegated'):
                objcls_kw['is_abstract'] = decl.delegated
            if hasattr(decl, 'final'):
                objcls_kw['final'] = decl.final

            if objcls is s_constr.Constraint:
                objcls_kw['return_type'] = self._schema.get('std::bool')
                objcls_kw['return_typemod'] = ft.TypeModifier.SINGLETON

            obj = objcls(name=name,
                         sourcectx=decl.context,
                         _setdefaults_=False,
                         _relaxrequired_=True,
                         **objcls_kw)

            for attrdecl in decl.attributes:
                attr_name = self._get_ref_name(attrdecl.name)

                if (hasattr(type(obj), attr_name) and
                        not isinstance(attrdecl.value, edgeql.ast.Base)):
                    value = self._get_literal_value(attrdecl.value)
                    # This is a builtin attribute, not an expression,
                    # simply set it on object.
                    self._schema = obj.set_attribute(
                        self._schema, attr_name, value,
                        source=attrdecl.value.context)

            self._schema = self._schema.add(obj)
            objects[type(obj)._type][obj] = decl

        # Second, process inheritance references.
        chain = itertools.chain.from_iterable
        for obj, decl in chain(t.items() for t in objects.values()):
            obj.bases = self._get_bases(obj, decl)

        # Now, with all objects in the declaration in the schema, we can
        # process them in the semantic dependency order.

        self._init_attributes(objects['attribute'])

        # Constraints have no external dependencies, but need to
        # be fully initialized when we get to constraint users below.
        self._init_constraints(objects['constraint'])
        constraints = self._sort(module.get_objects(type='constraint'))
        for constraint in constraints:
            self._schema = constraint.finalize(self._schema)

        # Ditto for attributes.
        attributes = self._sort(module.get_objects(type='attribute'))

        # ScalarTypes depend only on constraints and attributes,
        # can process them now.
        self._init_scalars(objects['ScalarType'])
        scalars = self._sort(
            module.get_objects(type='ScalarType'),
            depsfn=self._get_scalar_deps)

        # Generic links depend on scalars (via props), constraints
        # and attributes.
        self._init_links(objects['link'])

        # Finally, we can do the first pass on types
        self._init_objtypes(objects['ObjectType'])

        # The inheritance merge pass may produce additional objects,
        # thus, it has to be performed in reverse order (mostly).
        objtypes = self._sort(module.get_objects(type='ObjectType'))
        links = self._sort(module.get_objects(type='link'))
        props = self._sort(module.get_objects(type='property'))
        attrvals = module.get_objects(type='attribute-value')
        indexes = module.get_objects(type='index')

        constraints.update(c for c in module.get_objects(type='constraint')
                           if c.get_subject(self._schema) is not None)

        # Final pass, set empty fields to default values and do
        # other object finalization.

        for link, linkdecl in objects['link'].items():
            self._normalize_link_constraints(link, linkdecl)

        for objtype, objtypedecl in objects['ObjectType'].items():
            self._normalize_objtype_constraints(objtype, objtypedecl)

        # Arrange classes in the resulting schema according to determined
        # topological order.
        genlinks = [l for l in links if l.generic(self._schema)]
        speclinks = [l for l in links if not l.generic(self._schema)]

        self._schema = self._schema.reorder(itertools.chain(
            attributes, attrvals, constraints,
            scalars, props, indexes, genlinks, objtypes, speclinks))

        dctx = s_delta.CommandContext(declarative=True)

        for obj in module.get_objects():
            cmdcls = s_delta.ObjectCommandMeta.get_command_class_or_die(
                s_delta.CreateObject, type(obj))
            ctxcls = cmdcls.get_context_class()
            cmd = cmdcls(classname=obj.name)
            ctx = ctxcls(cmd, obj)
            with dctx(ctx):
                self._schema = obj.finalize(self._schema, dctx=dctx)

        # Normalization for defaults and other expressions must be
        # *after* finalize() so that all pointers have been inherited.
        for link, linkdecl in objects['link'].items():
            self._normalize_link_expressions(link, linkdecl)

        for objtype, objtypedecl in objects['ObjectType'].items():
            self._normalize_objtype_expressions(objtype, objtypedecl)

    def _process_imports(self, tree):
        for decl in tree.declarations:
            if isinstance(decl, s_ast.Import):
                for mod in decl.modules:
                    if not self._schema.has_module(mod.module):
                        raise s_err.SchemaError(
                            'cannot find module {!r}'.format(mod.module),
                            context=mod.context)
                    if mod.alias is not None:
                        self._mod_aliases[mod.alias] = mod.module

    def _sort(self, objects, depsfn=None):
        g = {}

        for obj in objects:
            this_item = g[obj.name] = {'item': obj, 'merge': [], 'deps': []}

            if depsfn is not None:
                deps = depsfn(obj)
                for dep in deps:
                    this_item['deps'].append(dep.name)
                    g[dep.name] = {'item': dep, 'merge': [], 'deps': []}

            if obj.bases:
                g[obj.name]['deps'].extend(b.name for b in obj.bases)

                for base in obj.bases:
                    if base.name.module != obj.name.module:
                        g[base.name] = {'item': base, 'merge': [], 'deps': []}

        if not g:
            return ordered.OrderedSet()

        item = next(iter(g.values()))['item']
        modname = item.name.module
        objs = topological.sort(g)
        return ordered.OrderedSet(filter(
            lambda obj: getattr(obj.name, 'module', None) == modname, objs))

    def _get_ref_name(self, ref):
        if isinstance(ref, edgeql.ast.ObjectRef):
            if ref.module:
                return s_name.Name(module=ref.module, name=ref.name)
            else:
                return ref.name
        else:
            raise TypeError('ObjectRef expected '
                            '(got type {!r})'.format(type(ref).__name__))

    def _get_ref_type(self, ref):
        clsname = self._get_ref_name(ref.maintype)
        if ref.subtypes:
            subtypes = [self._get_ref_type(s) for s in ref.subtypes]
            ccls = s_types.Collection.get_class(clsname)
            typ = ccls.from_subtypes(subtypes)
        else:
            try:
                typ = self._schema.get(
                    clsname, module_aliases=self._mod_aliases)
            except s_err.ItemNotFoundError as e:
                s_utils.enrich_schema_lookup_error(
                    e, clsname, modaliases=self._mod_aliases,
                    schema=self._schema, item_types=(s_types.Type,))
                e.set_source_context(ref.context)
                raise e

        return typ

    def _get_literal_value(self, node):
        if not isinstance(node, edgeql.ast.BaseConstant):
            raise TypeError('Literal expected '
                            '(got type {!r})'.format(type(node).__name__))

        return node.value

    def _get_bases(self, obj, decl):
        """Resolve object bases from the "extends" declaration."""
        bases = []

        if decl.extends:
            # Explicit inheritance
            for base_ref in decl.extends:
                base_name = self._get_ref_name(base_ref.maintype)

                base = self._schema.get(base_name, type=obj.__class__,
                                        module_aliases=self._mod_aliases)
                if base.is_final:
                    msg = '{!r} is final and cannot be inherited ' \
                          'from'.format(base.name)
                    raise s_err.SchemaError(msg, context=decl)

                bases.append(base)

        elif obj.name not in type(obj).get_root_classes():
            # Implicit inheritance from the default base class
            default_base_name = type(obj).get_default_base_name()
            if default_base_name is not None:
                default_base = self._schema.get(
                    default_base_name, module_aliases=self._mod_aliases)
                bases.append(default_base)

        return s_obj.ObjectList(bases)

    def _init_constraints(self, constraints):
        for constraint, decl in constraints.items():
            attrs = {a.name.name: a.value for a in decl.attributes}
            assert 'subject' not in attrs  # TODO: Add proper validation
            assert 'subjectexpr' not in attrs  # TODO: Add proper validation

            expr = attrs.pop('expr', None)
            if expr is not None:
                self._schema = constraint.set_field_value(
                    self._schema, 'expr', s_expr.ExpressionText(
                        qlcodegen.generate_source(expr)))

            subjexpr = decl.subject
            if subjexpr is not None:
                self._schema = constraint.set_field_value(
                    self._schema, 'subjectexpr', s_expr.ExpressionText(
                        qlcodegen.generate_source(subjexpr)))

            params = s_func.FuncParameterList.from_ast(
                self._schema, decl, self._mod_aliases,
                func_fqname=constraint.name)

            for param in params:
                if param.get_kind(self._schema) is ft.ParameterKind.NAMED_ONLY:
                    raise s_err.SchemaDefinitionError(
                        'named only parameters are not allowed '
                        'in this context',
                        context=decl.context)

                if param.get_default(self._schema) is not None:
                    raise s_err.SchemaDefinitionError(
                        'constraints do not support parameters '
                        'with defaults',
                        context=decl.context)

            self._schema = constraint.set_field_value(
                self._schema, 'params', params)

    def _init_attributes(self, attrs):
        for attr, attrdecl in attrs.items():
            self._schema = attr.set_field_value(
                self._schema, 'type', self._get_ref_type(attrdecl.type))

    def _init_scalars(self, scalars):
        for scalar, scalardecl in scalars.items():
            if scalardecl.attributes:
                self._parse_field_setters(scalar, scalardecl.attributes)

            if scalardecl.constraints:
                self._parse_subject_constraints(scalar, scalardecl)

    def _get_scalar_deps(self, scalar):
        deps = set()

        consts = scalar.get_constraints(self._schema)
        if not consts:
            return deps

        for constraint in consts.objects(self._schema):
            constraint_params = constraint.get_params(self._schema)
            if constraint_params:
                deps.update([p.get_type(self._schema)
                             for p in constraint_params])

        for dep in list(deps):
            if isinstance(dep, s_types.Collection):
                deps.update(dep.get_subtypes())
                deps.discard(dep)

        # Add dependency on all builtin scalars unconditionally
        std = self._schema.get_module('std')
        deps.update(std.get_objects(type='ScalarType'))

        return deps

    def _init_links(self, links):
        for link, decl in links.items():
            self._parse_source_props(link, decl)

    def _parse_source_props(self, source, sourcedecl):
        for propdecl in sourcedecl.properties:
            prop_name = self._get_ref_name(propdecl.name)
            prop_base = self._schema.get(prop_name,
                                         type=s_props.Property,
                                         default=None,
                                         module_aliases=self._mod_aliases)
            prop_target = None

            if prop_base is None:
                if not source.generic(self._schema):
                    # Only generic links can implicitly define properties
                    raise s_err.SchemaError('reference to an undefined '
                                            'property {!r}'.format(prop_name))

                # The link property has not been defined globally.
                if not s_name.Name.is_qualified(prop_name):
                    # If the name is not fully qualified, assume inline link
                    # property definition. The only attribute that is used for
                    # global definition is the name.
                    prop_qname = s_name.Name(
                        name=prop_name, module=source.name.module)

                    std_lprop = self._schema.get(
                        s_props.Property.get_default_base_name(),
                        module_aliases=self._mod_aliases)

                    prop_base = s_props.Property(
                        name=prop_qname, bases=[std_lprop])

                    self._schema = self._schema.add(prop_base)
                else:
                    prop_qname = s_name.Name(prop_name)
            else:
                prop_qname = prop_base.name

            if propdecl.target is not None:
                prop_target = self._get_ref_type(propdecl.target[0])
                if not isinstance(prop_target, (s_scalars.ScalarType,
                                                s_types.Collection)):
                    raise s_err.SchemaDefinitionError(
                        f'invalid property target, expected primitive type, '
                        f'got {prop_target.__class__.__name__}',
                        context=propdecl.target[0].context
                    )

            elif not source.generic(self._schema):
                link_base = source.bases[0]
                propdef = link_base.getptr(self._schema, prop_qname)
                if not propdef:
                    raise s_err.SchemaError(
                        'link {!r} does not define property '
                        '{!r}'.format(source.name, prop_qname))

                prop_qname = propdef.shortname

            new_props = {
                'sourcectx': propdecl.context,
            }

            self._schema, prop = prop_base.derive(
                self._schema, source, prop_target,
                attrs=new_props,
                add_to_schema=True)

            self._schema = prop.update(self._schema, {
                'declared_inherited': propdecl.inherited,
                'required': bool(propdecl.required),
                'cardinality': propdecl.cardinality,
            })

            if propdecl.expr is not None:
                self._schema = prop.set_field_value(
                    self._schema, 'computable', True)

            self._schema = source.add_pointer(self._schema, prop)

            if propdecl.attributes:
                self._parse_field_setters(prop, propdecl.attributes)

            if propdecl.constraints:
                self._parse_subject_constraints(prop, propdecl)

    def _parse_field_setters(self, ptr,
                             attrdecls: typing.List[s_ast.Attribute]):
        fields = type(ptr).get_fields()
        updates = {}

        check_type = lambda t, types: any(issubclass(bt, t) for bt in types)

        for attrdecl in attrdecls:
            attrname = attrdecl.name.name

            attrfield = fields.get(attrname)
            if attrfield is None or not attrfield.public:
                raise s_err.SchemaError(
                    f'unexpected attribute {attrname}',
                    context=attrdecl.context)

            if check_type(s_expr.ExpressionText, attrfield.type):
                updates[attrname] = qlcodegen.generate_source(attrdecl.value)

            elif (check_type(bool, attrfield.type) and
                    isinstance(attrdecl.value, qlast.BooleanConstant)):
                updates[attrname] = attrdecl.value.value.lower() == 'true'

            elif (check_type(str, attrfield.type) and
                    isinstance(attrdecl.value, qlast.StringConstant)):
                updates[attrname] = ql_lexutils.unescape_string(
                    attrdecl.value.value)
            else:
                raise s_err.SchemaError(
                    f'unable to parse value for {attrname} attribute',
                    context=attrdecl.context)

        if updates:
            self._schema = ptr.update(self._schema, updates)

    def _parse_subject_constraints(self, subject, subjdecl):
        # Perform initial collection of constraints defined in subject context.
        # At this point all referenced constraints should be fully initialized.

        constr = {}

        for constrdecl in subjdecl.constraints:
            attrs = {a.name.name: a.value for a in constrdecl.attributes}
            assert 'subject' not in attrs  # TODO: Add proper validation

            constr_name = self._get_ref_name(constrdecl.name)
            constr_base = self._schema.get(constr_name,
                                           type=s_constr.Constraint,
                                           module_aliases=self._mod_aliases)

            self._schema, constraint = constr_base.derive(
                self._schema, subject,
                attrs={
                    'is_abstract': constrdecl.delegated,
                    'sourcectx': constrdecl.context,
                })

            self._schema = constraint.acquire_ancestor_inheritance(
                self._schema)

            if constrdecl.args:
                args = [qlcodegen.generate_source(arg, pretty=False)
                        for arg in constrdecl.args]
            else:
                args = []

            subjectexpr = constrdecl.subject
            if subjectexpr is not None:
                self._schema = constraint.set_attribute(
                    self._schema,
                    'subjectexpr',
                    s_constr.Constraint.normalize_constraint_expr(
                        self._schema, {}, subjectexpr, subject=subject,
                        constraint=constraint),
                    source_context=constrdecl.subject.context,
                )

            self._schema = s_constr.Constraint.process_specialized_constraint(
                self._schema, constraint, args)

            # There can be only one specialized constraint per constraint
            # class per subject. At this point all placeholders have been
            # folded, so it is possible to merge the constraints consistently
            # by merging their final exprs.
            #
            try:
                prev = constr[constraint.bases[0].name]
            except KeyError:
                constr[constraint.bases[0].name] = constraint
            else:
                self._schema = constraint.merge(prev, schema=self._schema)
                self._schema = constraint.merge_localexprs(
                    prev, schema=self._schema)
                constr[constraint.bases[0].name] = constraint

        for c in constr.values():
            # Note that we don't do finalization for the constraint
            # here, since it's possible that it will be further used
            # in a merge of it's subject.
            #
            self._schema = self._schema.add(c)
            self._schema = subject.add_constraint(self._schema, c)

    def _parse_subject_indexes(self, subject, subjdecl):
        module_aliases = {None: subject.name.module}

        for indexdecl in subjdecl.indexes:
            index_name = self._get_ref_name(indexdecl.name)
            index_name = subject.name + '.' + index_name
            local_name = s_indexes.SourceIndex.get_specialized_name(
                index_name, subject.name)

            der_name = s_name.Name(name=local_name, module=subject.name.module)

            _, _, index_expr = qlutils.normalize_tree(
                indexdecl.expression, self._schema,
                modaliases=module_aliases,
                anchors={qlast.Subject: subject},
                inline_anchors=True)

            index = s_indexes.SourceIndex(
                name=der_name, expr=index_expr, subject=subject)

            self._schema = subject.add_index(self._schema, index)
            self._schema = self._schema.add(index)

    def _init_objtypes(self, objtypes):
        for objtype, objtypedecl in objtypes.items():
            self._parse_source_props(objtype, objtypedecl)

            if objtypedecl.attributes:
                self._parse_field_setters(objtype, objtypedecl.attributes)

            for linkdecl in objtypedecl.links:
                link_name = self._get_ref_name(linkdecl.name)
                link_base = self._schema.get(link_name, type=s_links.Link,
                                             default=None,
                                             module_aliases=self._mod_aliases)
                if link_base is None:
                    # The link has not been defined globally.
                    if not s_name.Name.is_qualified(link_name):
                        # If the name is not fully qualified, assume inline
                        # link definition. The only attribute that is used for
                        # global definition is the name.
                        link_qname = s_name.Name(
                            name=link_name, module=objtype.name.module)

                        std_link = self._schema.get(
                            s_links.Link.get_default_base_name(),
                            module_aliases=self._mod_aliases)

                        link_base = s_links.Link(
                            name=link_qname, bases=[std_link])

                        self._schema = self._schema.add(link_base)
                    else:
                        link_qname = s_name.Name(link_name)
                else:
                    link_qname = link_base.name

                if linkdecl.expr is not None:
                    # This is a computable, but we cannot interpret
                    # the expression yet, so set the target to `any`
                    # temporarily.
                    _targets = [s_pseudo.Any()]

                else:
                    _targets = [self._get_ref_type(t) for t in linkdecl.target]

                if len(_targets) == 1:
                    # Usual case, just one target
                    spectargets = []
                    target = _targets[0]
                else:
                    # Multiple explicit targets, create common virtual
                    # parent and use it as target.
                    spectargets = s_obj.ObjectSet(_targets)
                    target = link_base.get_common_target(
                        self._schema, spectargets)
                    if not self._schema.get(target.name, default=None):
                        self._schema = self._schema.add(target)

                if (not target.is_any() and
                        not isinstance(target, s_objtypes.ObjectType)):
                    raise s_err.SchemaDefinitionError(
                        f'invalid link target, expected object type, got '
                        f'{target.__class__.__name__}',
                        context=linkdecl.target[0].context
                    )

                new_props = {
                    'sourcectx': linkdecl.context,
                }

                self._schema, link = link_base.derive(
                    self._schema, objtype, target,
                    attrs=new_props,
                    add_to_schema=True,
                    apply_defaults=not linkdecl.inherited)

                self._schema = link.update(self._schema, {
                    'spectargets': spectargets,
                    'required': bool(linkdecl.required),
                    'cardinality': linkdecl.cardinality,
                    'declared_inherited': linkdecl.inherited,
                })

                if linkdecl.on_target_delete is not None:
                    self._schema = link.set_field_value(
                        self._schema,
                        'on_target_delete',
                        linkdecl.on_target_delete.cascade)

                if linkdecl.expr is not None:
                    self._schema = link.set_field_value(
                        self._schema, 'computable', True)

                self._parse_source_props(link, linkdecl)
                self._schema = objtype.add_pointer(self._schema, link)

        for objtype, objtypedecl in objtypes.items():
            if objtypedecl.indexes:
                self._parse_subject_indexes(objtype, objtypedecl)

            if objtypedecl.constraints:
                self._parse_subject_constraints(objtype, objtypedecl)

    def _normalize_link_constraints(self, link, linkdecl):
        if linkdecl.constraints:
            self._parse_subject_constraints(link, linkdecl)

    def _normalize_link_expressions(self, link, linkdecl):
        """Interpret and validate EdgeQL expressions in link declaration."""
        for propdecl in linkdecl.properties:
            if propdecl.expr is not None:
                # Computable
                prop_name = self._get_ref_name(propdecl.name)
                generic_prop = self._schema.get(
                    prop_name, module_aliases=self._mod_aliases)
                spec_prop = link.getptr(self._schema, generic_prop.name)

                if propdecl.expr is not None:
                    # Computable
                    self._normalize_ptr_default(
                        propdecl.expr, link, spec_prop, propdecl)

    def _normalize_objtype_constraints(self, objtype, objtypedecl):
        for linkdecl in objtypedecl.links:
            if linkdecl.constraints:
                link_name = self._get_ref_name(linkdecl.name)
                generic_link = self._schema.get(
                    link_name, module_aliases=self._mod_aliases)
                spec_link = objtype.getptr(self._schema, generic_link.name)
                self._parse_subject_constraints(spec_link, linkdecl)

    def _normalize_objtype_expressions(self, objtype, typedecl):
        """Interpret and validate EdgeQL expressions in type declaration."""
        for ptrdecl in itertools.chain(typedecl.links, typedecl.properties):
            link_name = self._get_ref_name(ptrdecl.name)
            generic_link = self._schema.get(
                link_name, module_aliases=self._mod_aliases)
            spec_link = objtype.getptr(self._schema, generic_link.name)

            if ptrdecl.expr is not None:
                # Computable
                self._normalize_ptr_default(
                    ptrdecl.expr, objtype, spec_link, ptrdecl)

            for attr in ptrdecl.attributes:
                name = attr.name.name
                if name == 'default':
                    if isinstance(attr.value, edgeql.ast.SelectQuery):
                        self._normalize_ptr_default(
                            attr.value, objtype, spec_link, ptrdecl)
                    else:
                        expr = attr.value
                        _, _, default = qlutils.normalize_tree(
                            expr, self._schema)
                        self._schema = spec_link.set_field_value(
                            self._schema, 'default', default)

    def _normalize_ptr_default(self, expr, source, ptr, ptrdecl):
        module_aliases = {None: source.name.module}

        ir, _, expr_text = qlutils.normalize_tree(
            expr, self._schema,
            modaliases=module_aliases,
            anchors={qlast.Source: source})

        self_set = ast.find_children(
            ir, lambda n: getattr(n, 'anchor', None) == qlast.Source,
            terminate_early=True)

        try:
            expr_type = ir_utils.infer_type(ir, self._schema)
        except edgeql.EdgeQLError as e:
            raise s_err.SchemaError(
                'could not determine the result type of the default '
                'expression on {!s}.{!s}'.format(
                    source.name, ptr.shortname),
                context=expr.context) from e

        self._schema = ptr.set_field_value(self._schema, 'default', expr_text)

        if ptr.is_pure_computable(self._schema):
            # Pure computable without explicit target.
            # Fixup pointer target and target property.
            self._schema = ptr.set_field_value(
                self._schema, 'target', expr_type)

            if isinstance(ptr, s_links.Link):
                if not isinstance(expr_type, s_objtypes.ObjectType):
                    raise s_err.SchemaDefinitionError(
                        f'invalid link target, expected object type, got '
                        f'{expr_type.__class__.__name__}',
                        context=ptrdecl.expr.context
                    )
            else:
                if not isinstance(expr_type, (s_scalars.ScalarType,
                                              s_types.Collection)):
                    raise s_err.SchemaDefinitionError(
                        f'invalid property target, expected primitive type, '
                        f'got {expr_type.__class__.__name__}',
                        context=ptrdecl.expr.context
                    )

            if isinstance(ptr, s_links.Link):
                pname = s_name.Name('std::target')
                tgt_prop = ptr.getptr(self._schema, pname)
                self._schema = tgt_prop.set_field_value(
                    self._schema, 'target', expr_type)

            scope_tree_root = ir_ast.new_scope_tree()
            if self_set is not None:
                scope_tree_root.attach_path(self_set.path_id)
                scope_tree = scope_tree_root.attach_fence()
            else:
                scope_tree = scope_tree_root

            self._schema = ptr.set_field_value(
                self._schema,
                'cardinality',
                ir_inference.infer_cardinality(ir, scope_tree, self._schema))

            if ptrdecl.cardinality is not ptr.get_cardinality(self._schema):
                if ptrdecl.cardinality is qlast.Cardinality.ONE:
                    raise s_err.SchemaError(
                        f'computable expression possibly returns more than '
                        f'one value, but the {ptr.schema_class_displayname!r} '
                        f'is declared as "single"',
                        context=expr.context)

        if (not isinstance(expr_type, s_types.Type) or
                (ptr.get_target(self._schema) is not None and
                 not expr_type.issubclass(
                    self._schema, ptr.get_target(self._schema)))):
            raise s_err.SchemaError(
                'default value query must yield a single result of '
                'type {!r}'.format(ptr.get_target(self._schema).name),
                context=expr.context)


def load_module_declarations(schema, declarations):
    """Create a schema and populate it with provided declarations."""
    loader = DeclarationLoader(schema)

    for module_name, decl_ast in declarations:
        loader.load_module(module_name, decl_ast)

    return schema


def parse_module_declarations(schema, declarations):
    """Create a schema and populate it with provided declarations."""
    loader = DeclarationLoader(schema)

    for module_name, declaration in declarations:
        decl_ast = s_parser.parse(declaration)
        loader.load_module(module_name, decl_ast)

    return schema
