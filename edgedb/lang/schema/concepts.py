##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import constraints
from . import delta as sd
from . import inheriting
from . import links
from . import name as sn
from . import named
from . import nodes
from . import referencing
from . import sources
from . import types as s_types


class SourceNode(sources.Source, nodes.Node):
    pass


class Concept(SourceNode, constraints.ConsistencySubject):
    _type = 'concept'

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

    class ReversePointerResolver:
        @classmethod
        def getptr_from_nqname(cls, schema, source, name):
            ptrs = set()

            for link in schema.get_objects(type='link'):
                if (link.shortname.name == name and
                        link.target is not None and
                        source.issubclass(link.target)):
                    ptrs.add(link)

            return ptrs

        @classmethod
        def getptr_from_fqname(cls, schema, source, name):
            ptrs = set()

            for link in schema.get_objects(type='link'):
                if (link.shortname == name and
                        link.target is not None and
                        source.issubclass(link.target)):
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
                                  base_ptr_class, skip_atomic):
            result = set()
            for link in schema.get_objects(type='link'):
                if link.issubclass(base_ptr_class) \
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

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='Object')
        )

    @classmethod
    def get_default_base_name(cls):
        return sn.Name(module='std', name='Object')


class VirtualConcept(SourceNode,
                     constraints.ConsistencySubject, s_types.Type):
    pass


class DerivedConcept(SourceNode,
                     constraints.ConsistencySubject, s_types.Type):
    pass


class ConceptCommandContext(sd.ClassCommandContext,
                            constraints.ConsistencySubjectCommandContext,
                            links.LinkSourceCommandContext,
                            nodes.NodeCommandContext):
    pass


class ConceptCommand(constraints.ConsistencySubjectCommand,
                     sources.SourceCommand, links.LinkSourceCommand,
                     nodes.NodeCommand,
                     schema_metaclass=Concept,
                     context_class=ConceptCommandContext):
    def _apply_field_ast(self, context, node, op):
        if op.property == 'is_derived':
            pass
        else:
            super()._apply_field_ast(context, node, op)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)
        cmd = cls._handle_view_op(cmd, astnode, context, schema)
        return cmd


class CreateConcept(ConceptCommand, inheriting.CreateInheritingClass):
    astnode = qlast.CreateConcept


class RenameConcept(ConceptCommand, named.RenameNamedClass):
    pass


class RebaseConcept(ConceptCommand, referencing.RebaseReferencingClass):
    pass


class AlterConcept(ConceptCommand, inheriting.AlterInheritingClass):
    astnode = qlast.AlterConcept


class DeleteConcept(ConceptCommand, inheriting.DeleteInheritingClass):
    astnode = qlast.DropConcept
