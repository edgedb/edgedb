##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""Facility for loading EdgeDB module declarations into a schema."""

import collections
import itertools

from edgedb.lang.common import ast
from edgedb.lang.common import ordered
from edgedb.lang.common import topological

from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import codegen as edgeql_codegen
from edgedb.lang.edgeql import codegen as qlcodegen

from edgedb.lang.ir import utils as ir_utils
from edgedb.lang.ir import inference as ir_inference

from . import ast as s_ast
from . import parser as s_parser

from . import attributes as s_attrs
from . import delta as s_delta
from . import objtypes as s_objtypes
from . import constraints as s_constr
from . import error as s_err
from . import expr as s_expr
from . import indexes as s_indexes
from . import links as s_links
from . import lproperties as s_props
from . import modules as s_mod
from . import name as s_name
from . import objects as s_obj
from . import pointers as s_pointers
from . import scalars as s_scalars
from . import schema as s_schema
from . import types as s_types
from . import utils as s_utils


_DECL_MAP = {
    s_ast.ScalarTypeDeclaration: s_scalars.ScalarType,
    s_ast.ObjectTypeDeclaration: s_objtypes.ObjectType,
    s_ast.ConstraintDeclaration: s_constr.Constraint,
    s_ast.LinkDeclaration: s_links.Link,
}


class DeclarationLoader:
    def __init__(self, schema):
        self._schema = schema
        self._mod_aliases = {}

    def load_module(self, module_name, decl_ast):
        decls = decl_ast.declarations

        self._module = module = s_mod.Module(name=module_name)
        self._schema.add_module(module)
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
                    setattr(obj, attr_name, value)

            self._schema.add(obj)
            objects[type(obj)._type][obj] = decl

        # Second, process inheritance references.
        chain = itertools.chain.from_iterable
        for obj, decl in chain(t.items() for t in objects.values()):
            obj.bases = self._get_bases(obj, decl)

        # Now, with all objects in the declaration in the schema, we can
        # process them in the semantic dependency order.

        # Constraints have no external dependencies, but need to
        # be fully initialized when we get to constraint users below.
        self._init_constraints(objects['constraint'])
        constraints = self._sort(module.get_objects(type='constraint'))

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

        # Finaly, we can do the first pass on types
        self._init_objtypes(objects['ObjectType'])

        # The inheritance merge pass may produce additional objects,
        # thus, it has to be performed in reverse order (mostly).
        objtypes = self._sort(module.get_objects(type='ObjectType'))
        links = self._sort(module.get_objects(type='link'))
        props = self._sort(module.get_objects(type='property'))
        events = self._sort(module.get_objects(type='event'))
        actions = self._sort(module.get_objects(type='action'))
        attrvals = module.get_objects(type='attribute-value')
        indexes = module.get_objects(type='index')

        constraints.update(c for c in module.get_objects(type='constraint')
                           if c.subject is not None)

        # Final pass, set empty fields to default values and do
        # other object finalization.

        for link, linkdecl in objects['link'].items():
            self._normalize_link_constraints(link, linkdecl)

        for objtype, objtypedecl in objects['ObjectType'].items():
            self._normalize_objtype_constraints(objtype, objtypedecl)

        # Arrange classes in the resulting schema according to determined
        # topological order.
        self._schema.reorder(itertools.chain(
            attributes, attrvals, actions, events, constraints,
            scalars, props, indexes, links, objtypes))

        dctx = s_delta.CommandContext(declarative=True)

        for obj in module.get_objects():
            cmdcls = s_delta.ObjectCommandMeta.get_command_class_or_die(
                s_delta.CreateObject, type(obj))
            ctxcls = cmdcls.get_context_class()
            cmd = cmdcls(classname=obj.name)
            ctx = ctxcls(cmd, obj)
            with dctx(ctx):
                obj.finalize(self._schema, dctx=dctx)

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
        return ordered.OrderedSet(
            filter(lambda obj: obj.name.module == modname, objs))

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
        if not isinstance(node, edgeql.ast.Constant):
            raise TypeError('Literal expected '
                            '(got type {!r})'.format(type(node).__name__))

        return node.value

    def _get_literal_attribute(self, node, name):
        for attr in node.attributes:
            if attr.name.name == name:
                return self._get_literal_value(attr.value)

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
        module_aliases = {}

        for constraint, decl in constraints.items():
            attrs = {a.name.name: a.value for a in decl.attributes}
            assert 'subject' not in attrs  # TODO: Add proper validation
            assert 'subjectexpr' not in attrs  # TODO: Add proper validation

            expr = attrs.pop('expr', None)
            if expr is not None:
                try:
                    expr = s_constr.Constraint.normalize_constraint_expr(
                        self._schema, module_aliases, expr)
                except (ValueError, edgeql.EdgeQLError) as e:
                    raise s_err.SchemaError(e.args[0], context=decl) from None

                constraint.expr = expr

            subjexpr = decl.subject
            if subjexpr is not None:
                try:
                    subjexpr = s_constr.Constraint.normalize_constraint_expr(
                        self._schema, module_aliases, subjexpr)
                except (ValueError, edgeql.EdgeQLError) as e:
                    raise s_err.SchemaError(e.args[0], context=decl) from None

                constraint.subjectexpr = subjexpr

    def _init_scalars(self, scalars):
        for scalar, scalardecl in scalars.items():
            if scalardecl.attributes:
                self._parse_attribute_values(scalar, scalardecl)

            if scalardecl.constraints:
                self._parse_subject_constraints(scalar, scalardecl)

    def _get_scalar_deps(self, scalar):
        deps = set()

        if scalar.constraints:
            for constraint in scalar.constraints.values():
                if constraint.paramtypes:
                    deps.update(constraint.paramtypes)

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
                if not source.generic():
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

                    self._schema.add(prop_base)
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

            elif not source.generic():
                link_base = source.bases[0]
                propdef = link_base.pointers.get(prop_qname)
                if not propdef:
                    raise s_err.SchemaError(
                        'link {!r} does not define property '
                        '{!r}'.format(source.name, prop_qname))

                prop_qname = propdef.shortname

            prop = prop_base.derive(self._schema, source, prop_target,
                                    add_to_schema=True)

            prop.sourcectx = propdecl.context

            prop.declared_inherited = propdecl.inherited
            prop.required = bool(propdecl.required)

            cardinality = self._get_literal_attribute(
                propdecl, 'cardinality')
            if cardinality is not None:
                prop.cardinality = cardinality

            if propdecl.expr is not None:
                prop.computable = True

            source.add_pointer(prop)

            if propdecl.attributes:
                for attrdecl in propdecl.attributes:
                    name = attrdecl.name.name
                    if name == 'default':
                        # the default can be computable or static
                        #
                        self._parse_ptr_default(attrdecl.value, source, prop)
                        break

            if propdecl.constraints:
                self._parse_subject_constraints(prop, propdecl)

    def _parse_ptr_default(self, expr, source, ptr):
        """Set the default value for a pointer."""
        if not isinstance(expr, edgeql.ast.SelectQuery):
            expr = edgeql.ast.Constant(value=self._get_literal_value(expr))

        ptr.default = s_expr.ExpressionText(qlcodegen.generate_source(expr))

    def _parse_attribute_values(self, subject, subjdecl):
        attrs = {}

        for attrdecl in subjdecl.attributes:
            attr_name = self._get_ref_name(attrdecl.name)
            value = self._get_literal_value(attrdecl.value)

            if hasattr(type(subject), attr_name):
                # This is a builtin attribute should have already been set
                continue

            attribute = self._schema.get(
                attr_name, module_aliases=self._mod_aliases)

            if (attribute.type.is_container and
                    not isinstance(value, list)):
                value = [value]

            genname = s_attrs.AttributeValue.get_specialized_name(
                attribute.name, subject.name)

            dername = s_name.Name(name=genname, module=subject.name.module)

            try:
                value = attribute.type.coerce(value)
            except ValueError as e:
                msg = e.args[0].format(name=attribute.name.name)
                context = attrdecl.context
                raise s_err.SchemaError(msg, context=context) from e

            attrvalue = s_attrs.AttributeValue(
                name=dername, subject=subject,
                attribute=attribute, value=value)
            attrvalue.sourcectx = attrdecl.context

            self._schema.add(attrvalue)
            subject.add_attribute(attrvalue)

        return attrs

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

            constraint = constr_base.derive(self._schema, subject)
            constraint.is_abstract = constrdecl.delegated
            constraint.acquire_ancestor_inheritance(self._schema)
            constraint.sourcectx = constrdecl.context

            args = None
            if constrdecl.args:
                args = ','.join([
                    edgeql_codegen.generate_source(arg, pretty=False)
                    for arg in constrdecl.args])
                args = f'({args},)'

            subjectexpr = constrdecl.subject
            if subjectexpr is not None:
                constraint.subjectexpr = \
                    s_constr.Constraint.normalize_constraint_expr(
                        self._schema, {}, subjectexpr, subject=subject)

            s_constr.Constraint.process_specialized_constraint(
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
                constraint.merge(prev, schema=self._schema)
                constraint.merge_localexprs(prev, schema=self._schema)
                constr[constraint.bases[0].name] = constraint

        for c in constr.values():
            # Note that we don't do finalization for the constraint
            # here, since it's possible that it will be further used
            # in a merge of it's subject.
            #
            self._schema.add(c)
            subject.add_constraint(c)

    def _parse_subject_indexes(self, subject, subjdecl):
        module_aliases = {None: subject.name.module}

        for indexdecl in subjdecl.indexes:
            index_name = self._get_ref_name(indexdecl.name)
            index_name = subject.name + '.' + index_name
            local_name = s_indexes.SourceIndex.get_specialized_name(
                index_name, subject.name)

            der_name = s_name.Name(name=local_name, module=subject.name.module)

            _, _, index_expr = edgeql.utils.normalize_tree(
                indexdecl.expression, self._schema,
                modaliases=module_aliases,
                anchors={qlast.Subject: subject},
                inline_anchors=True)

            index = s_indexes.SourceIndex(
                name=der_name, expr=index_expr, subject=subject)

            subject.add_index(index)
            self._schema.add(index)

    def _init_objtypes(self, objtypes):
        for objtype, objtypedecl in objtypes.items():
            self._parse_source_props(objtype, objtypedecl)

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

                        self._schema.add(link_base)
                    else:
                        link_qname = s_name.Name(link_name)
                else:
                    link_qname = link_base.name

                if linkdecl.expr is not None:
                    # This is a computable, but we cannot interpret
                    # the expression yet, so set the target to `any`
                    # temporarily.
                    _targets = [self._schema.get('std::any')]

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
                        self._schema.add(target)

                if (target.name != 'std::any' and
                        not isinstance(target, s_objtypes.ObjectType)):
                    raise s_err.SchemaDefinitionError(
                        f'invalid link target, expected object type, got '
                        f'{target.__class__.__name__}',
                        context=linkdecl.target[0].context
                    )

                link = link_base.derive(
                    self._schema, objtype, target, add_to_schema=True)

                link.sourcectx = linkdecl.context

                link.spectargets = spectargets

                link.required = bool(linkdecl.required)
                link.declared_inherited = linkdecl.inherited

                cardinality = self._get_literal_attribute(
                    linkdecl, 'cardinality')
                if cardinality is not None:
                    link.cardinality = cardinality

                if linkdecl.expr is not None:
                    link.computable = True

                self._parse_source_props(link, linkdecl)
                objtype.add_pointer(link)

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
                spec_prop = link.pointers[generic_prop.name]

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
                spec_link = objtype.pointers[generic_link.name]
                self._parse_subject_constraints(spec_link, linkdecl)

    def _normalize_objtype_expressions(self, objtype, typedecl):
        """Interpret and validate EdgeQL expressions in type declaration."""
        for ptrdecl in itertools.chain(typedecl.links, typedecl.properties):
            link_name = self._get_ref_name(ptrdecl.name)
            generic_link = self._schema.get(
                link_name, module_aliases=self._mod_aliases)
            spec_link = objtype.pointers[generic_link.name]

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
                        expr = edgeql.ast.Constant(
                            value=self._get_literal_value(attr.value))
                        _, _, spec_link.default = edgeql.utils.normalize_tree(
                            expr, self._schema)

    def _normalize_ptr_default(self, expr, source, ptr, ptrdecl):
        module_aliases = {None: source.name.module}

        ir, _, expr_text = edgeql.utils.normalize_tree(
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

        ptr.default = expr_text
        ptr.normalize_defaults()

        if ptr.is_pure_computable():
            # Pure computable without explicit target.
            # Fixup pointer target and target property.
            ptr.target = expr_type

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
                tgt_prop = ptr.pointers[pname]
                tgt_prop.target = expr_type

            cardinality = self._get_literal_attribute(ptrdecl, 'cardinality')
            if cardinality is not None:
                raise s_err.SchemaError(
                    'computable links must not define explicit cardinality',
                    context=expr.context)

            singletons = set()
            if self_set is not None:
                singletons.add(self_set)

            cardinality = \
                ir_inference.infer_cardinality(ir, singletons, self._schema)

            if cardinality == qlast.Cardinality.MANY:
                ptr.cardinality = s_pointers.PointerCardinality.ManyToMany
            else:
                ptr.cardinality = s_pointers.PointerCardinality.ManyToOne

        if (not isinstance(expr_type, s_types.Type) or
                (ptr.target is not None and
                 not expr_type.issubclass(ptr.target))):
            raise s_err.SchemaError(
                'default value query must yield a single result of '
                'type {!r}'.format(ptr.target.name), context=expr.context)

        if not isinstance(ptr.target, s_scalars.ScalarType):
            many_mapping = (s_pointers.PointerCardinality.ManyToOne,
                            s_pointers.PointerCardinality.ManyToMany)
            if ptr.cardinality not in many_mapping:
                raise s_err.SchemaError(
                    'type links with query defaults '
                    'must have either a "*1" or "**" cardinality',
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
