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

from edb import errors

from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import codegen as qlcodegen
from edb.lang.edgeql import compiler as qlcompiler
from edb.lang.edgeql import functypes as ft
from edb.lang.edgeql import utils as qlutils

from . import ast as s_ast
from . import parser as s_parser

from . import abc as s_abc
from . import attributes as s_attrs
from . import delta as s_delta
from . import objtypes as s_objtypes
from . import constraints as s_constr
from . import expr as s_expr
from . import functions as s_func
from . import indexes as s_indexes
from . import links as s_links
from . import lproperties as s_props
from . import modules as s_mod
from . import name as s_name
from . import objects as s_obj
from . import ordering as s_ordering
from . import pointers as s_pointers
from . import pseudo as s_pseudo
from . import scalars as s_scalars
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

        self._schema, self._module = s_mod.Module.create_in_schema(
            self._schema, name=module_name)
        self._mod_aliases[None] = module_name

        self._process_imports(decl_ast)

        order = s_ordering.get_global_dep_order()

        objects = collections.OrderedDict(
            (s_objtypes.ObjectType if t is s_objtypes.BaseObjectType else t,
             collections.OrderedDict()) for t in order)

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

            # TODO: refactor this
            objcls_kw = {}
            if hasattr(decl, 'abstract'):
                objcls_kw['is_abstract'] = decl.abstract
            if hasattr(decl, 'delegated'):
                objcls_kw['is_abstract'] = decl.delegated
            if hasattr(decl, 'final'):
                objcls_kw['is_final'] = decl.final
            if hasattr(decl, 'inheritable'):
                objcls_kw['inheritable'] = decl.inheritable

            if objcls is s_constr.Constraint:
                objcls_kw['return_type'] = self._schema.get('std::bool')
                objcls_kw['return_typemod'] = ft.TypeModifier.SINGLETON

            if issubclass(objcls, s_pointers.Pointer):
                if len(decl.name) > s_pointers.MAX_NAME_LENGTH:
                    raise errors.SchemaDefinitionError(
                        f'link or property name length exceeds the maximum of '
                        f'{s_pointers.MAX_NAME_LENGTH} characters',
                        context=decl.context)

            self._schema, obj = objcls.create_in_schema(
                self._schema,
                name=name,
                sourcectx=decl.context,
                **objcls_kw,
            )

            if decl.attributes:
                self._parse_attr_setters(obj, decl.attributes)

            objects[type(obj)][obj] = decl

        # Second, process inheritance references.
        chain = itertools.chain.from_iterable
        for obj, decl in chain(t.items() for t in objects.values()):
            self._schema = obj.set_field_value(
                self._schema, 'bases', self._get_bases(obj, decl))

        # Now, with all objects in the declaration in the schema, we can
        # process them in the semantic dependency order.

        self._init_attributes(objects[s_attrs.Attribute])

        # Constraints have no external dependencies, but need to
        # be fully initialized when we get to constraint users below.
        self._init_constraints(objects[s_constr.Constraint])
        constraints = self._schema.get_objects(
            modules=[module_name], type=s_constr.Constraint)
        constraints = s_ordering.sort_objects(self._schema, constraints)
        for constraint in constraints:
            self._schema = constraint.finalize(self._schema)

        # ScalarTypes depend only on constraints and attributes,
        # can process them now.
        self._init_scalars(objects[s_scalars.ScalarType])

        # Generic links depend on scalars (via props), constraints
        # and attributes.
        self._init_links(objects[s_links.Link])

        # Finally, we can do the first pass on types
        self._init_objtypes(objects[s_objtypes.ObjectType])

        constraints.update(c for c in self._schema.get_objects(
            modules=[module_name], type=s_constr.Constraint)
            if c.get_subject(self._schema) is not None)

        # Final pass, set empty fields to default values and do
        # other object finalization.

        for link, linkdecl in objects[s_links.Link].items():
            self._normalize_link_constraints(link, linkdecl)

        for objtype, objtypedecl in objects[s_objtypes.ObjectType].items():
            self._normalize_objtype_constraints(objtype, objtypedecl)

        dctx = s_delta.CommandContext(declarative=True)

        everything = s_ordering.sort_objects(
            self._schema, self._schema.get_objects(modules=[module_name]))

        for obj in everything:
            cmdcls = s_delta.ObjectCommandMeta.get_command_class_or_die(
                s_delta.CreateObject, type(obj))
            ctxcls = cmdcls.get_context_class()
            cmd = cmdcls(classname=obj.get_name(self._schema))
            ctx = ctxcls(self._schema, cmd, obj)
            with dctx(ctx):
                self._schema = obj.finalize(self._schema, dctx=dctx)

        # Normalization for defaults and other expressions must be
        # *after* finalize() so that all pointers have been inherited.
        for link, linkdecl in objects[s_links.Link].items():
            self._normalize_link_expressions(link, linkdecl)

        for objtype, objtypedecl in objects[s_objtypes.ObjectType].items():
            self._normalize_objtype_expressions(objtype, objtypedecl)

        return self._schema

    def _process_imports(self, tree):
        for decl in tree.declarations:
            if isinstance(decl, s_ast.Import):
                for mod in decl.modules:
                    if not self._schema.has_module(mod.module):
                        raise errors.SchemaError(
                            'cannot find module {!r}'.format(mod.module),
                            context=mod.context)
                    if mod.alias is not None:
                        self._mod_aliases[mod.alias] = mod.module

    def _get_ref_name(self, ref):
        if isinstance(ref, edgeql.ast.ObjectRef):
            if ref.module:
                return s_name.Name(module=ref.module, name=ref.name)
            else:
                return ref.name
        else:
            raise TypeError('ObjectRef expected '
                            '(got type {!r})'.format(type(ref).__name__))

    def _get_ref_obj(self, ref, item_type):
        clsname = self._get_ref_name(ref)
        try:
            obj = self._schema.get(
                clsname, type=item_type, module_aliases=self._mod_aliases)
        except errors.InvalidReferenceError as e:
            s_utils.enrich_schema_lookup_error(
                e, clsname, modaliases=self._mod_aliases,
                schema=self._schema, item_types=(item_type,))
            e.set_source_context(ref.context)
            raise e

        return obj

    def _get_ref_type(self, ref):
        clsname = self._get_ref_name(ref.maintype)
        if ref.subtypes:
            subtypes = [self._get_ref_type(s) for s in ref.subtypes]
            ccls = s_types.Collection.get_class(clsname)
            typ = ccls.from_subtypes(self._schema, subtypes)
        else:
            try:
                typ = self._schema.get(
                    clsname, module_aliases=self._mod_aliases)
            except errors.InvalidReferenceError as e:
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
                if base.get_is_final(self._schema):
                    msg = '{!r} is final and cannot be inherited ' \
                          'from'.format(base.get_name(self._schema))
                    raise errors.SchemaError(msg, context=decl)

                bases.append(base)

        elif obj.get_name(self._schema) not in type(obj).get_root_classes():
            # Implicit inheritance from the default base class
            default_base_name = type(obj).get_default_base_name()
            if default_base_name is not None:
                default_base = self._schema.get(
                    default_base_name, module_aliases=self._mod_aliases)
                bases.append(default_base)

        return s_obj.ObjectList.create(self._schema, bases)

    def _init_constraints(self, constraints):
        for constraint, decl in constraints.items():
            attrs = {a.name.name: a.value for a in decl.fields}
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

            self._schema, params = s_func.FuncParameterList.from_ast(
                self._schema, decl, self._mod_aliases,
                func_fqname=constraint.get_name(self._schema))

            for param in params.objects(self._schema):
                if param.get_kind(self._schema) is ft.ParameterKind.NAMED_ONLY:
                    raise errors.InvalidConstraintDefinitionError(
                        'named only parameters are not allowed '
                        'in this context',
                        context=decl.context)

                if param.get_default(self._schema) is not None:
                    raise errors.InvalidConstraintDefinitionError(
                        'constraints do not support parameters '
                        'with defaults',
                        context=decl.context)

            self._schema = constraint.set_field_value(
                self._schema, 'params', params)

    def _init_attributes(self, attrs):
        pass

    def _init_scalars(self, scalars):
        for scalar, scalardecl in scalars.items():
            if scalardecl.fields:
                self._parse_field_setters(scalar, scalardecl.fields)

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
                             for p in constraint_params.objects(self._schema)])

        for dep in list(deps):
            if isinstance(dep, s_abc.Collection):
                deps.update(dep.get_subtypes())
                deps.discard(dep)

        # Add dependency on all builtin scalars unconditionally
        deps.update(self._schema.get_objects(
            modules=['std'], type=s_scalars.ScalarType))

        return deps

    def _init_links(self, links):
        for link, decl in links.items():
            self._parse_source_props(link, decl)

    def _parse_source_props(self, source, sourcedecl):
        for propdecl in sourcedecl.properties:
            prop_name = self._get_ref_name(propdecl.name)
            if len(prop_name) > s_pointers.MAX_NAME_LENGTH:
                raise errors.SchemaDefinitionError(
                    f'link or property name length exceeds the maximum of '
                    f'{s_pointers.MAX_NAME_LENGTH} characters',
                    context=propdecl.context)

            prop_base = self._schema.get(prop_name,
                                         type=s_props.Property,
                                         default=None,
                                         module_aliases=self._mod_aliases)
            prop_target = None

            if prop_base is None:
                if not source.generic(self._schema):
                    # Only generic links can implicitly define properties
                    raise errors.SchemaDefinitionError(
                        f'reference to an undefined property {prop_name!r}')

                # The link property has not been defined globally.
                if not s_name.Name.is_qualified(prop_name):
                    # If the name is not fully qualified, assume inline link
                    # property definition. The only attribute that is used for
                    # global definition is the name.
                    prop_qname = s_name.Name(
                        name=prop_name,
                        module=source.get_name(self._schema).module)

                    std_lprop = self._schema.get(
                        s_props.Property.get_default_base_name(),
                        module_aliases=self._mod_aliases)

                    self._schema, prop_base = \
                        s_props.Property.create_in_schema(
                            self._schema,
                            name=prop_qname,
                            bases=[std_lprop])

                else:
                    prop_qname = s_name.Name(prop_name)
            else:
                prop_qname = prop_base.get_name(self._schema)

            if propdecl.target is not None:
                prop_target = self._get_ref_type(propdecl.target[0])
                if not isinstance(prop_target, (s_scalars.ScalarType,
                                                s_types.Collection)):
                    raise errors.InvalidPropertyTargetError(
                        f'invalid property target, expected primitive type, '
                        f'got {prop_target.__class__.__name__}',
                        context=propdecl.target[0].context
                    )

            elif not source.generic(self._schema):
                link_base = source.get_bases(self._schema).first(self._schema)
                propdef = link_base.getptr(self._schema, prop_qname.name)
                if not propdef:
                    raise errors.SchemaError(
                        'link {!r} does not define property '
                        '{!r}'.format(
                            source.get_name(self._schema),
                            prop_qname.name))

                prop_qname = propdef.get_shortname(self._schema)

            new_props = {
                'sourcectx': propdecl.context,
            }

            self._schema, prop = prop_base.derive(
                self._schema, source, prop_target,
                attrs=new_props)

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
                self._parse_attr_setters(prop, propdecl.attributes)

            if propdecl.fields:
                self._parse_field_setters(prop, propdecl.fields)

            if propdecl.constraints:
                self._parse_subject_constraints(prop, propdecl)

    def _parse_attr_setters(
            self, scls, attrdecls: typing.List[s_ast.Attribute]):
        for attrdecl in attrdecls:
            attr = self._get_ref_obj(attrdecl.name, s_attrs.Attribute)
            value = qlcompiler.evaluate_ast_to_python_val(
                attrdecl.value, self._schema, modaliases=self._mod_aliases)

            if not isinstance(value, str):
                raise errors.SchemaDefinitionError(
                    'attribute value is not a string',
                    context=attrdecl.value.context)

            self._schema = scls.set_attribute(self._schema, attr, value)

    def _parse_field_setters(
            self, scls, field_decls: typing.List[s_ast.Field]):
        fields = type(scls).get_fields()
        updates = {}

        for field_decl in field_decls:
            fieldname = field_decl.name.name

            attrfield = fields.get(fieldname)
            if attrfield is None or not attrfield.allow_ddl_set:
                raise errors.SchemaError(
                    f'unexpected field {fieldname}',
                    context=field_decl.context)

            if issubclass(attrfield.type, s_expr.ExpressionText):
                updates[fieldname] = qlcodegen.generate_source(
                    field_decl.value)

            else:
                updates[fieldname] = qlcompiler.evaluate_ast_to_python_val(
                    field_decl.value, self._schema,
                    modaliases=self._mod_aliases)

        if updates:
            self._schema = scls.update(self._schema, updates)

    def _parse_subject_constraints(self, subject, subjdecl):
        # Perform initial collection of constraints defined in subject context.
        # At this point all referenced constraints should be fully initialized.

        for constrdecl in subjdecl.constraints:
            attrs = {a.name.name: a.value for a in constrdecl.fields}
            assert 'subject' not in attrs  # TODO: Add proper validation

            constr_name = self._get_ref_name(constrdecl.name)
            if constrdecl.args:
                args = [
                    qlcodegen.generate_source(arg, pretty=False)
                    for arg in constrdecl.args
                ]
            else:
                args = []

            modaliases = {None: subject.get_name(self._schema).module}
            self._schema, c, _ = \
                s_constr.Constraint.create_concrete_constraint(
                    self._schema,
                    subject,
                    name=constr_name,
                    is_abstract=constrdecl.delegated,
                    sourcectx=constrdecl.context,
                    subjectexpr=constrdecl.subject,
                    args=args,
                    modaliases=modaliases,
                )

            self._schema = subject.add_constraint(self._schema, c)

    def _parse_subject_indexes(self, subject, subjdecl):
        module_aliases = {None: subject.get_name(self._schema).module}

        for indexdecl in subjdecl.indexes:
            index_name = self._get_ref_name(indexdecl.name)
            index_name = subject.get_name(self._schema) + '.' + index_name
            local_name = s_name.get_specialized_name(
                index_name, subject.get_name(self._schema))

            der_name = s_name.Name(
                name=local_name, module=subject.get_name(self._schema).module)

            _, _, index_expr = qlutils.normalize_tree(
                indexdecl.expression, self._schema,
                modaliases=module_aliases,
                anchors={qlast.Subject: subject},
                inline_anchors=True)

            self._schema, index = s_indexes.SourceIndex.create_in_schema(
                self._schema,
                name=der_name,
                expr=index_expr,
                subject=subject
            )

            self._schema = subject.add_index(self._schema, index)

    def _init_objtypes(self, objtypes):
        for objtype, objtypedecl in objtypes.items():
            self._parse_source_props(objtype, objtypedecl)

            if objtypedecl.fields:
                self._parse_field_setters(objtype, objtypedecl.fields)

            for linkdecl in objtypedecl.links:
                link_name = self._get_ref_name(linkdecl.name)
                if len(link_name) > s_pointers.MAX_NAME_LENGTH:
                    raise errors.SchemaDefinitionError(
                        f'link or property name length exceeds the maximum of '
                        f'{s_pointers.MAX_NAME_LENGTH} characters',
                        context=linkdecl.context)

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
                            name=link_name,
                            module=objtype.get_name(self._schema).module)

                        std_link = self._schema.get(
                            s_links.Link.get_default_base_name(),
                            module_aliases=self._mod_aliases)

                        self._schema, link_base = \
                            s_links.Link.create_in_schema(
                                self._schema,
                                name=link_qname,
                                bases=[std_link])

                    else:
                        link_qname = s_name.Name(link_name)
                else:
                    link_qname = link_base.get_name(self._schema)

                if linkdecl.expr is not None:
                    # This is a computable, but we cannot interpret
                    # the expression yet, so set the target to `any`
                    # temporarily.
                    _targets = [s_pseudo.Any.create()]

                else:
                    _targets = [self._get_ref_type(t) for t in linkdecl.target]

                if len(_targets) == 1:
                    # Usual case, just one target
                    spectargets = None
                    target = _targets[0]
                else:
                    # Multiple explicit targets, create common virtual
                    # parent and use it as target.
                    spectargets = s_obj.ObjectSet.create(
                        self._schema, _targets)
                    self._schema, target = link_base.create_common_target(
                        self._schema, _targets)

                if (not target.is_any() and
                        not isinstance(target, s_objtypes.ObjectType)):
                    raise errors.InvalidLinkTargetError(
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
                spec_prop = link.getptr(
                    self._schema,
                    generic_prop.get_name(self._schema).name)

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
                spec_link = objtype.getptr(
                    self._schema, generic_link.get_name(self._schema).name)
                self._parse_subject_constraints(spec_link, linkdecl)

    def _normalize_objtype_expressions(self, objtype, typedecl):
        """Interpret and validate EdgeQL expressions in type declaration."""
        for ptrdecl in itertools.chain(typedecl.links, typedecl.properties):
            link_name = self._get_ref_name(ptrdecl.name)
            generic_link = self._schema.get(
                link_name, module_aliases=self._mod_aliases)
            spec_link = objtype.getptr(
                self._schema, generic_link.get_name(self._schema).name)

            if ptrdecl.expr is not None:
                # Computable
                self._normalize_ptr_default(
                    ptrdecl.expr, objtype, spec_link, ptrdecl)

            for attr in ptrdecl.fields:
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
        module_aliases = {None: source.get_name(self._schema).module}

        ir, _, expr_text = qlutils.normalize_tree(
            expr, self._schema,
            modaliases=module_aliases,
            anchors={qlast.Source: source},
            singletons=[source])

        expr_type = ir.stype

        self._schema = ptr.set_field_value(self._schema, 'default', expr_text)

        if ptr.is_pure_computable(self._schema):
            # Pure computable without explicit target.
            # Fixup pointer target and target property.
            self._schema = ptr.set_field_value(
                self._schema, 'target', expr_type)

            if isinstance(ptr, s_links.Link):
                if not isinstance(expr_type, s_objtypes.ObjectType):
                    raise errors.InvalidLinkTargetError(
                        f'invalid link target, expected object type, got '
                        f'{expr_type.__class__.__name__}',
                        context=ptrdecl.expr.context
                    )
            else:
                if not isinstance(expr_type, (s_scalars.ScalarType,
                                              s_types.Collection)):
                    raise errors.InvalidPropertyTargetError(
                        f'invalid property target, expected primitive type, '
                        f'got {expr_type.__class__.__name__}',
                        context=ptrdecl.expr.context
                    )

            if isinstance(ptr, s_links.Link):
                tgt_prop = ptr.getptr(self._schema, 'target')
                self._schema = tgt_prop.set_field_value(
                    self._schema, 'target', expr_type)

            self._schema = ptr.set_field_value(
                self._schema, 'cardinality', ir.cardinality)

            if ptrdecl.cardinality is not ptr.get_cardinality(self._schema):
                if ptrdecl.cardinality is qlast.Cardinality.ONE:
                    raise errors.SchemaError(
                        f'computable expression possibly returns more than '
                        f'one value, but the {ptr.schema_class_displayname!r} '
                        f'is declared as "single"',
                        context=expr.context)

        if (not isinstance(expr_type, s_abc.Type) or
                (ptr.get_target(self._schema) is not None and
                 not expr_type.issubclass(
                    self._schema, ptr.get_target(self._schema)))):
            raise errors.SchemaError(
                'default value query must yield a single result of '
                'type {!r}'.format(
                    ptr.get_target(self._schema).get_name(self._schema)),
                context=expr.context)


def load_module_declarations(schema, declarations):
    """Create a schema and populate it with provided declarations."""
    loader = DeclarationLoader(schema)

    for module_name, decl_ast in declarations:
        schema = loader.load_module(module_name, decl_ast)

    return schema


def parse_module_declarations(schema, declarations):
    """Create a schema and populate it with provided declarations."""
    loader = DeclarationLoader(schema)

    for module_name, declaration in declarations:
        decl_ast = s_parser.parse(declaration)
        schema = loader.load_module(module_name, decl_ast)

    return schema
