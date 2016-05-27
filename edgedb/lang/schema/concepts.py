##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.lang.caosql import ast as qlast

from . import constraints
from . import delta as sd
from . import indexes
from . import inheriting
from . import links
from . import name as sn
from . import named
from . import objects as so
from . import pointers
from . import sources


class ConceptCommandContext(sd.PrototypeCommandContext,
                            constraints.ConsistencySubjectCommandContext,
                            links.LinkSourceCommandContext):
    pass


class ConceptCommand(sd.PrototypeCommand):
    context_class = ConceptCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Concept

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(pointers.PointerCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(indexes.SourceIndexCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)


class CreateConcept(ConceptCommand, named.CreateNamedPrototype):
    astnode = qlast.CreateConceptNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        if astnode.is_abstract:
            cmd.add(sd.AlterPrototypeProperty(
                property='is_abstract',
                new_value=True
            ))

        if astnode.is_final:
            cmd.add(sd.AlterPrototypeProperty(
                property='is_final',
                new_value=True
            ))

        return cmd

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        proto = super().apply(schema, context)

        with context(ConceptCommandContext(self, proto)):
            for op in self(pointers.PointerCommand):
                op.apply(schema, context=context)

            for op in self(indexes.SourceIndexCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

        proto.acquire_ancestor_inheritance(schema)

        return proto


class RenameConcept(ConceptCommand, named.RenameNamedPrototype):
    pass


class RebaseConcept(ConceptCommand, inheriting.RebaseNamedPrototype):
    def apply(self, schema, context):
        concept = super().apply(schema, context)

        concepts = [concept] + list(concept.descendants(schema))
        for concept in concepts:
            for pointer_name in concept.pointers.copy():
                if pointer_name not in concept.own_pointers:
                    try:
                        concept.get_pointer_origin(pointer_name)
                    except KeyError:
                        del concept.pointers[pointer_name]

        return concept


class AlterConcept(ConceptCommand, named.AlterNamedPrototype):
    astnode = qlast.AlterConceptNode

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        with context(ConceptCommandContext(self, None)):
            concept = super().apply(schema, context)

            for op in self(inheriting.RebaseNamedPrototype):
                op.apply(schema, context)

            concept.acquire_ancestor_inheritance(schema)

            for op in self(pointers.PointerCommand):
                op.apply(schema, context=context)

            for op in self(indexes.SourceIndexCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

        return concept


class DeleteConcept(ConceptCommand, named.DeleteNamedPrototype):
    astnode = qlast.DropConceptNode

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()
        concept = super().apply(schema, context)

        with context(ConceptCommandContext(self, concept)):
            for op in self(pointers.PointerCommand):
                op.apply(schema, context=context)

            for op in self(indexes.SourceIndexCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

        return concept


class Concept(sources.Source, constraints.ConsistencySubject, so.ProtoNode):
    _type = 'concept'

    is_virtual = so.Field(bool, default=bool, compcoef=0.5)

    delta_driver = sd.DeltaDriver(
        create=CreateConcept,
        alter=AlterConcept,
        rebase=RebaseConcept,
        rename=RenameConcept,
        delete=DeleteConcept
    )

    @classmethod
    def get_pointer_class(cls):
        return links.Link

    def materialize_policies(self, schema):
        bases = self.bases

        for link_name in self.pointers:
            own = self.own_pointers.get(link_name)
            ro = list(filter(lambda i: i is not None,
                             [b.pointers.get(link_name) for b in bases
                                                        if not b.is_virtual]))
            if own is not None and ro:
                own._merge_policies(schema, ro, force_first=True)

    def get_metaclass(self, proto_schema):
        from metamagic.caos.concept import ConceptMeta
        return ConceptMeta

    class ReversePointerResolver:
        @classmethod
        def getptr_from_nqname(cls, schema, source, name):
            ptrs = set()

            for link in schema('link'):
                if link.normal_name().name == name and link.target is not None \
                                        and source.issubclass(link.target):
                    ptrs.add(link)

            return ptrs

        @classmethod
        def getptr_from_fqname(cls, schema, source, name):
            ptrs = set()

            for link in schema('link'):
                if link.normal_name() == name and link.target is not None \
                                        and source.issubclass(link.target):
                    ptrs.add(link)

            return ptrs

        @classmethod
        def getptr(cls, schema, source, name):
            if sn.Name.is_qualified(name):
                return cls.getptr_from_fqname(schema, source, name)
            else:
                return cls.getptr_from_nqname(schema, source, name)

        @classmethod
        def getptr_inherited_from(cls, source, schema,
                                  base_ptr_proto, skip_atomic):
            result = set()
            for link in schema('link'):
                if link.issubclass(base_ptr_proto) \
                        and link.target is not None \
                        and (not skip_atomic or not link.atomic()) \
                        and source.issubclass(link.target):
                    result.add(link)
            return result

    def getrptr_descending(self, schema, name):
        return self._getptr_descending(schema, name,
                                       self.__class__.ReversePointerResolver)

    def getrptr_ascending(self, schema, name, include_inherited=False):
        return self._getptr_ascending(schema, name,
                                      self.__class__.ReversePointerResolver,
                                      include_inherited=include_inherited)

    def get_searchable_links(self):
        names = sorted(self.pointers.keys())

        for link_name in names:
            link_set = self.pointers[link_name]
            for link in link_set:
                if getattr(link, 'search', None):
                    yield link_name, link_set
                    break
