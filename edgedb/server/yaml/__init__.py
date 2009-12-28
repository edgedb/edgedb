import copy

import importlib
import collections
import itertools

from semantix.utils import graph
from semantix import lang
from semantix.caos import MetaError
from semantix.caos.name import Name as CaosName

from semantix.caos.backends import meta
from semantix.caos.backends.meta import RealmMeta


class AtomModExpr(meta.AtomModExpr, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        return cls(data['expr'], context=context)


class AtomModMinLength(meta.AtomModMinLength, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        return cls(data['min-length'], context=context)


class AtomModMaxLength(meta.AtomModMaxLength, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        return cls(data['max-length'], context=context)


class AtomModRegExp(meta.AtomModRegExp, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        return cls(data['regexp'], context=context)


class Atom(meta.Atom, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        atom = cls(name=None, backend=data.get('backend'), base=data['extends'], default=data['default'])
        atom.context = context
        mods = data.get('mods')
        if mods:
            for mod in mods:
                atom.add_mod(mod)
        return atom


class Concept(meta.Concept, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [CaosName(name=extends, module=context.module.__name__)]
            else:
                extends = [CaosName(name=e, module=context.module.__name__) for e in extends]

        concept = cls(name=None, backend=data.get('backend'), base=extends)
        concept.context = context
        for link in data['links']:
            concept.add_link(link)
        return concept


class ConceptLink(meta.ConceptLink, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        name, info = next(iter(data.items()))
        if isinstance(info, str):
            return cls(source=None, targets={info}, link_type=name)
        else:
            targets = set(info.keys())
            info = next(iter(info.values()))
            result = cls(source=None, targets=targets, link_type=name, mapping=info['mapping'], required=info['required'])
            result.mods = info.get('mods')
            return result


class OrderedDict(collections.OrderedDict, lang.meta.Object):
    @classmethod
    def construct(cls, data, context):
        return cls(data)


class MetaSet(lang.meta.Object):
    def __init__(self, data, context):
        self.context = context
        self.metaindex = RealmMeta()
        self.metaindex.add_module(context.module.__name__, None)

        backend = data.get('backend')

        for atom_name, atom in data['atoms'].items():
            atom.name = CaosName(name=atom_name, module=atom.context.module.__name__)
            atom_base = self.metaindex.get(atom.base)
            atom.base = atom_base.name
            atom.backend = backend
            self.metaindex.add(atom)

        concepts = graph.normalize(self.read_concepts(data, self.metaindex), merger=self.merge_concepts)

        for concept in concepts:
            self.metaindex.add(concept)

        for concept in concepts:
            links = {}
            link_target_types = {}

            for link_name, link in concept.links.items():
                if not isinstance(link.source, meta.GraphObject):
                    link.source = self.metaindex.get(link.source)

                targets = set()

                for target in link.targets:
                    if isinstance(target, meta.GraphObject):
                        # Inherited link
                        targets.add(target)
                        continue

                    target_obj = self.metaindex.get(target)

                    if not target_obj:
                        raise MetaError('reference to an undefined node "%s" in "%s"' %
                                        (target, str(concept.name) + '/links/' + link_name))

                    if (link_name, target) in links:
                        raise MetaError('%s --%s--> %s link redefinition' % (concept.name, link_name, target))

                    targets.add(target_obj)

                    if isinstance(target_obj, meta.Atom):
                        if link_name in link_target_types and link_target_types[link_name] != 'atom':
                            raise MetaError('%s link is already defined as a link to non-atom')

                        mods = getattr(link, 'mods', None)
                        if mods:
                            # Got an inline atom definition.
                            # We must generate a unique name here
                            atom_name = '__' + concept.name.name + '__' + link_name
                            atom = Atom(name=CaosName(name=atom_name, module=concept.name.module),
                                        base=target_obj.name,
                                        default=getattr(link, 'default', None), automatic=True,
                                        backend=concept.backend)
                            for mod in link.mods:
                                atom.add_mod(mod)
                            self.metaindex.add(atom)

                            targets = {atom}

                        if link.mapping != '11':
                            raise MetaError('%s: links to atoms can only have a "1 to 1" mapping' % link_name)

                        link_target_types[link_name] = 'atom'
                    else:
                        if link_name in link_target_types and link_target_types[link_name] == 'atom':
                            raise MetaError('%s link is already defined as a link to atom')

                        link_target_types[link_name] = 'concept'
                link.targets = targets

    def read_concepts(self, data, meta):
        concept_graph = {}

        for concept_name, concept in data['concepts'].items():
            concept.name = CaosName(name=concept_name, module=concept.context.module.__name__)
            concept.backend = data.get('backend')

            for link in concept.links.values():
                link.source = concept.name

            if meta.get(concept.name, None):
                raise MetaError('%s already defined' % concept.name)

            concept_graph[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                concept_graph[concept.name]["merge"].extend(concept.base)

        return concept_graph

    @staticmethod
    def merge_concepts(left, right):
        right.merge(left)
        return right

    @classmethod
    def construct(cls, data, context):
        return cls(data, context)

    def items(self):
        return itertools.chain([('_index_', self.metaindex)], self.metaindex.index_by_name.items())


class Backend(meta.MetaBackend):

    def __init__(self, source_path):
        super().__init__()
        self.metadata = importlib.import_module(source_path)

    def getmeta(self):
        return self.metadata._index_
