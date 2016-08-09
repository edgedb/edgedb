##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""Facility for loading EdgeDB module declarations into a schema."""

import collections
import itertools

from edgedb.lang.common import datastructures
from edgedb.lang.common.algos import topological

from edgedb.lang import edgeql

from . import ast as s_ast
from . import parser as s_parser

from . import atoms as s_atoms
from . import attributes as s_attrs
from . import concepts as s_concepts
from . import constraints as s_constr
from . import error as s_err
from . import indexes as s_indexes
from . import links as s_links
from . import lproperties as s_lprops
from . import modules as s_mod
from . import name as s_name
from . import objects as s_obj
from . import schema as s_schema


_DECL_MAP = {
    s_ast.AtomDeclaration: s_atoms.Atom,
    s_ast.ConceptDeclaration: s_concepts.Concept,
    s_ast.ConstraintDeclaration: s_constr.Constraint,
    s_ast.LinkDeclaration: s_links.Link,
}


class DeclarationLoader:
    def __init__(self, schema):
        self._schema = schema

    def load_module(self, module_name, decl_ast):
        decls = decl_ast.declarations

        module = s_mod.ProtoModule(name=module_name)
        self._schema.add_module(module, alias=None)

        self._process_imports(decl_ast)

        order = s_schema.ProtoSchema.global_dep_order
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
            obj = objcls(name=name, is_abstract=decl.abstract,
                         final=decl.final, _setdefaults_=False,
                         _relaxrequired_=True)

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
        constraints = self._merge_and_sort(module('constraint'))

        # Ditto for attributes.
        attributes = self._merge_and_sort(module('attribute'))

        # Atoms depend only on constraints and attributes,
        # can process them now.
        self._init_atoms(objects['atom'])
        atoms = self._merge_and_sort(
            module('atom'), depsfn=self._get_atom_deps)

        # Generic links depend on atoms (via props), constraints
        # and attributes.
        self._init_links(objects['link'])

        # Finaly, we can do the first pass on concepts
        self._init_concepts(objects['concept'])

        # The inheritance merge pass may produce additional objects,
        # thus, it has to be performed in reverse order (mostly).
        concepts = self._merge_and_sort(module('concept'))
        links = self._merge_and_sort(module('link'))
        linkprops = self._merge_and_sort(module('linkproperty'))
        events = self._merge_and_sort(module('event'))
        actions = self._merge_and_sort(module('action'))
        attrvals = module('attribute-value')
        indexes = module('index')

        constraints.update(c for c in module('constraint')
                           if c.subject is not None)

        # Final pass, set empty fields to default values amd do
        # other object finalization.

        for link, linkdecl in objects['link'].items():
            self._normalize_link_expressions(link, linkdecl)

        for concept, conceptdecl in objects['concept'].items():
            self._normalize_concept_expressions(concept, conceptdecl)

        for obj in module():
            obj.finalize(self._schema)

        # Arrange prototypes in the resulting schema according to determined
        # topological order.
        self._schema.reorder(itertools.chain(
            attributes, attrvals, actions, events, constraints,
            atoms, linkprops, indexes, links, concepts))

    def _process_imports(self, tree):
        for decl in tree.declarations:
            if isinstance(decl, s_ast.Import):
                for mod in decl.modules:
                    if not self._schema.has_module(mod.module):
                        raise s_err.SchemaError(
                            'cannot find module {!r}'.format(mod.module),
                            context=mod.context)
                    if mod.alias is not None:
                        self._schema.set_module_alias(mod.module, mod.alias)

    def _merge_and_sort(self, objects, depsfn=None):
        g = {}

        for obj in objects:
            this_item = g[obj.name] = {'item': obj, 'merge': [], 'deps': []}

            if depsfn is not None:
                deps = depsfn(obj)
                for dep in deps:
                    this_item['deps'].append(dep.name)
                    g[dep.name] = {'item': dep, 'merge': [], 'deps': []}

            if obj.bases:
                g[obj.name]['merge'].extend(b.name for b in obj.bases)

                for base in obj.bases:
                    if base.name.module != obj.name.module:
                        g[base.name] = {'item': base, 'merge': [], 'deps': []}

        if not g:
            return datastructures.OrderedSet()

        item = next(iter(g.values()))['item']
        objmerger = type(item).merge
        modname = item.name.module
        objs = topological.normalize(g, merger=objmerger, schema=self._schema)
        return datastructures.OrderedSet(
            filter(lambda obj: obj.name.module == modname, objs))

    def _get_ref_name(self, ref):
        if isinstance(ref, s_ast.ObjectName):
            if ref.module:
                return s_name.Name(module=ref.module, name=ref.name)
            else:
                return ref.name
        else:
            raise TypeError('ObjectName expected '
                            '(got type {!r})'.format(type(ref).__name__))

    def _get_literal_value(self, node):
        if not isinstance(node, s_ast.LiteralNode):
            raise TypeError('LiteralNode expected '
                            '(got type {!r})'.format(type(node).__name__))

        return node.value

    def _get_bases(self, obj, decl):
        """Resolve object bases from the "extends" declaration."""
        bases = []

        if decl.extends:
            # Explicit inheritance
            for base_ref in decl.extends:
                base_name = self._get_ref_name(base_ref)

                base = self._schema.get(base_name, type=obj.__class__)
                if base.is_final:
                    msg = '{!r} is final and cannot be inherited ' \
                          'from'.format(base.name)
                    raise s_err.SchemaError(msg, context=decl)

                bases.append(base)
        else:
            # Implicit inheritance from the default base class
            default_base_name = type(obj).get_default_base_name()
            if default_base_name is not None and obj.name != default_base_name:
                default_base = self._schema.get(default_base_name)
                bases.append(default_base)

        return s_obj.PrototypeList(bases)

    def _init_constraints(self, constraints):
        module_aliases = {}

        for constraint, decl in constraints.items():
            attrs = {a.name.name: a.value for a in decl.attributes}
            expr = attrs.get('expr')
            if expr is not None:
                try:
                    expr = s_constr.Constraint.normalize_constraint_expr(
                        self._schema, module_aliases, expr)
                except (ValueError, edgeql.EdgeQLQueryError) as e:
                    raise s_err.SchemaError(e.args[0], context=decl) from None

                constraint.expr = expr

            subjexpr = attrs.get('subject')
            if subjexpr is not None:
                try:
                    subjexpr = s_constr.Constraint.normalize_constraint_expr(
                        self._schema, module_aliases, subjexpr)
                except (ValueError, edgeql.EdgeQLQueryError) as e:
                    raise s_err.SchemaError(e.args[0], context=decl) from None

                constraint.subjectexpr = subjexpr

    def _init_atoms(self, atoms):
        for atom, atomdecl in atoms.items():
            if atomdecl.attributes:
                self._parse_attribute_values(atom, atomdecl)

            if atomdecl.constraints:
                self._parse_subject_constraints(atom, atomdecl)

    def _get_atom_deps(self, atom):
        deps = set()

        if atom.constraints:
            for constraint in atom.constraints.values():
                if constraint.paramtypes:
                    deps.update(constraint.paramtypes.values())
                inferred = constraint.inferredparamtypes
                if inferred:
                    deps.update(inferred.values())

            deps = {
                p.element_type if isinstance(p, s_obj.Collection) else p
                for p in deps
            }

            # Add dependency on all builtin atoms unconditionally
            std = self._schema.get_module('std')
            deps.update(std('atom'))

        return deps

    def _init_links(self, links):
        for link, decl in links.items():
            self._parse_link_props(link, decl)

    def _parse_link_props(self, link, linkdecl):
        for propdecl in linkdecl.properties:
            prop_name = self._get_ref_name(propdecl.name)
            prop_base = self._schema.get(prop_name,
                                         type=s_lprops.LinkProperty,
                                         default=None)
            prop_target = None

            if prop_base is None:
                if not link.generic():
                    # Only generic links can implicitly define properties
                    raise s_err.SchemaError('reference to an undefined '
                                            'property {!r}'.format(prop_name))

                # The link property has not been defined globally.
                if not s_name.Name.is_qualified(prop_name):
                    # If the name is not fully qualified, assume inline link
                    # property definition. The only attribute that is used for
                    # global definition is the name.
                    prop_qname = s_name.Name(
                        name=prop_name, module=link.name.module)

                    std_lprop = self._schema.get(
                        s_lprops.LinkProperty.get_default_base_name())

                    prop_base = s_lprops.LinkProperty(
                        name=prop_qname, bases=[std_lprop])

                    self._schema.add(prop_base)
                else:
                    prop_qname = s_name.Name(prop_name)
            else:
                prop_qname = prop_base.name

            if propdecl.target is not None:
                target_name = self._get_ref_name(propdecl.target[0])
                prop_target = self._schema.get(target_name)

            elif not link.generic():
                link_base = link.bases[0]
                propdef = link_base.pointers.get(prop_qname)
                if not propdef:
                    raise s_err.SchemaError(
                        'link {!r} does not define property '
                        '{!r}'.format(link.name, prop_qname))

                prop_qname = propdef.normal_name()

            prop = prop_base.derive(self._schema, link, prop_target,
                                    add_to_schema=True)

            if propdecl.constraints:
                self._parse_subject_constraints(prop, propdecl)

            link.add_pointer(prop)

    def _parse_attribute_values(self, subject, subjdecl):
        attrs = {}

        for attrdecl in subjdecl.attributes:
            attr_name = self._get_ref_name(attrdecl.name)
            value = self._get_literal_value(attrdecl.value)

            if hasattr(type(subject), attr_name):
                # This is a builtin attribute should have already been set
                continue

            attribute = self._schema.get(attr_name)

            if (attribute.type.is_container and
                    not isinstance(value, list)):
                value = [value]

            genname = s_attrs.AttributeValue.generate_specialized_name(
                subject.name, attribute.name)

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

            self._schema.add(attrvalue)
            subject.add_attribute(attrvalue)

        return attrs

    def _parse_subject_constraints(self, subject, subjdecl):
        # Perform initial collection of constraints defined in subject context.
        # At this point all referenced constraints should be fully initialized.

        constr = {}

        for constrdecl in subjdecl.constraints:
            constr_name = self._get_ref_name(constrdecl.name)
            constr_base = self._schema.get(constr_name,
                                           type=s_constr.Constraint)

            constraint = constr_base.derive(self._schema, subject)
            constraint.acquire_ancestor_inheritance(self._schema)

            # We now have a full set of data to perform final validation
            # and analysis of the constraint.
            #
            args = {}
            if constrdecl.value is not None:
                args['param'] = self._get_literal_value(constrdecl.value)
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
            local_name = s_indexes.SourceIndex.generate_specialized_name(
                subject.name, index_name)

            der_name = s_name.Name(name=local_name, module=subject.name.module)

            _, _, index_expr = edgeql.utils.normalize_tree(
                indexdecl.expression, self._schema,
                module_aliases=module_aliases, anchors={'self': subject},
                inline_anchors=True)

            index = s_indexes.SourceIndex(
                name=der_name, expr=index_expr, subject=subject)

            subject.add_index(index)
            self._schema.add(index)

    def _init_concepts(self, concepts):
        for concept, conceptdecl in concepts.items():
            for linkdecl in conceptdecl.links:
                link_name = self._get_ref_name(linkdecl.name)
                link_base = self._schema.get(link_name, type=s_links.Link,
                                             default=None)
                if link_base is None:
                    # The link has not been defined globally.
                    if not s_name.Name.is_qualified(link_name):
                        # If the name is not fully qualified, assume inline
                        # link definition. The only attribute that is used for
                        # global definition is the name.
                        link_qname = s_name.Name(
                            name=link_name, module=concept.name.module)

                        std_link = self._schema.get(
                            s_links.Link.get_default_base_name())

                        link_base = s_links.Link(
                            name=link_qname, bases=[std_link])

                        self._schema.add(link_base)
                    else:
                        link_qname = s_name.Name(link_name)
                else:
                    link_qname = link_base.name

                if isinstance(linkdecl.target, edgeql.ast.SelectQueryNode):
                    # This is a computable, but we cannot interpret
                    # the expression yet, so set the target to none
                    # temporarily.
                    _tnames = ['std.none']

                elif isinstance(linkdecl.target, list):
                    _tnames = [self._get_ref_name(t) for t in
                               linkdecl.target]
                else:
                    _tnames = [self._get_ref_name(linkdecl.target)]

                if len(_tnames) == 1:
                    # Usual case, just one target
                    spectargets = None
                    target = self._schema.get(_tnames[0])
                else:
                    # Multiple explicit targets, create common virtual
                    # parent and use it as target.
                    spectargets = s_obj.PrototypeSet(
                        self._schema.get(t) for t in _tnames)
                    target = link_base.create_common_target(
                        self._schema, spectargets)
                    target.is_derived = True

                link = link_base.derive(
                    self._schema, concept, target, add_to_schema=True)

                if isinstance(linkdecl.target, edgeql.ast.SelectQueryNode):
                    # Computables are always readonly.
                    link.readonly = True

                self._parse_link_props(link, linkdecl)
                concept.add_pointer(link)

        for concept, conceptdecl in concepts.items():
            if conceptdecl.indexes:
                self._parse_subject_indexes(concept, conceptdecl)

            if conceptdecl.constraints:
                self._parse_subject_constraints(concept, conceptdecl)

    def _normalize_link_expressions(self, link, linkdecl):
        """Interpret and validate EdgeQL expressions in link declaration."""
        for propdecl in linkdecl.properties:
            if isinstance(propdecl.target, edgeql.ast.SelectQueryNode):
                # Computable
                prop_name = self._get_ref_name(propdecl.name)
                generic_prop = self._schema.get(prop_name)
                spec_prop = link.pointers[generic_prop.name]
                self._normalize_ptr_default(
                    linkdecl.target, link, spec_prop)

        if linkdecl.constraints:
            self._parse_subject_constraints(link, linkdecl)

    def _normalize_concept_expressions(self, concept, conceptdecl):
        """Interpret and validate EdgeQL expressions in concept declaration."""
        for linkdecl in conceptdecl.links:
            if isinstance(linkdecl.target, edgeql.ast.SelectQueryNode):
                # Computable
                link_name = self._get_ref_name(linkdecl.name)
                generic_link = self._schema.get(link_name)
                spec_link = concept.pointers[generic_link.name]
                self._normalize_ptr_default(
                    linkdecl.target, concept, spec_link)

    def _normalize_ptr_default(self, expr, source, ptr):
        module_aliases = {None: source.name.module}

        ir, _, expr_text = edgeql.utils.normalize_tree(
            expr, self._schema, module_aliases=module_aliases,
            anchors={'self': source})

        first = list(ir.result_types.values())[0][0]
        if first is None:
            raise s_err.SchemaError(
                'could not determine the result type of the default '
                'expression on {!s}.{!s}'.format(
                    source.name, ptr.normal_name()),
                context=expr.context)

        ptr.default = expr_text
        ptr.normalize_defaults()

        if ptr.is_pure_computable():
            # Pure computable without explicit target.
            # Fixup pointer target and target property.
            ptr.target = first

            if isinstance(ptr, s_links.Link):
                pname = s_name.Name('std.target')
                tgt_prop = ptr.pointers[pname]
                tgt_prop.target = first

        if (len(ir.result_types) > 1 or
                not isinstance(first, s_obj.ProtoNode) or
                (ptr.target is not None and not first.issubclass(ptr.target))):
            raise s_err.SchemaError(
                'default value query must yield a single result of '
                'type {!r}'.format(ptr.target.name), context=expr.context)

        if not isinstance(ptr.target, s_atoms.Atom):
            many_mapping = (s_links.LinkMapping.ManyToOne,
                            s_links.LinkMapping.ManyToMany)
            if ptr.mapping not in many_mapping:
                raise s_err.SchemaError(
                    'concept links with query defaults '
                    'must have either a "*1" or "**" mapping',
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
