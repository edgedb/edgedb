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
                extends = [extends]

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


class ImportContext(lang.ImportContext):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metaindex = RealmMeta()
        self.toplevel = False

    @classmethod
    def from_parent(cls, name, parent):
        result = cls(name)
        if parent and isinstance(parent, ImportContext):
            result.metaindex = parent.metaindex
            result.toplevel = False
        else:
            result.toplevel = True
        return result

    @classmethod
    def copy(cls, name, other):
        result = cls(name)
        if isinstance(other, ImportContext):
            result.metaindex = other.metaindex
            result.toplevel = other.toplevel
        return result

    @classmethod
    def construct(cls, name, toplevel=True):
        result = cls(name)
        result.toplevel = toplevel
        return result


class MetaSet(lang.meta.Object):
    def __init__(self, data, context):
        self.context = context
        self.finalindex = RealmMeta()

        self.toplevel = context.document.import_context.toplevel
        globalindex = context.document.import_context.metaindex

        localindex = RealmMeta()
        localindex.add_module(context.document.module.__name__, None)

        for alias, module in context.document.imports.items():
            localindex.add_module(module.__name__, alias)

        self.read_atoms(data, globalindex, localindex)
        self.read_concepts(data, globalindex, localindex)

        if self.toplevel:
            concepts = self.order_concepts(globalindex)
            atoms = self.order_atoms(globalindex)

            for atom in atoms:
                self.finalindex.add(atom)

            for concept in concepts:
                self.finalindex.add(concept)


    def read_atoms(self, data, globalmeta, localmeta):
        backend = data.get('backend')

        for atom_name, atom in data['atoms'].items():
            atom.name = CaosName(name=atom_name, module=atom.context.document.module.__name__)
            atom.backend = backend
            globalmeta.add(atom)
            localmeta.add(atom)

        for atom in localmeta('atom'):
            if atom.base:
                atom.base = localmeta.normalize_name(atom.base)


    def order_atoms(self, globalmeta):
        g = {}

        for atom in globalmeta('atom', include_automatic=True):
            g[atom.name] = {"item": atom, "merge": [], "deps": []}

            if atom.base:
                atom_base = globalmeta.get(atom.base)
                atom.base = atom_base.name
                if atom.base.module != 'builtin':
                    g[atom.name]['deps'].append(atom.base)

        return graph.normalize(g, merger=None)


    def read_concepts(self, data, globalmeta, localmeta):
        backend = data.get('backend')

        for concept_name, concept in data['concepts'].items():
            concept.name = CaosName(name=concept_name, module=concept.context.document.module.__name__)
            concept.backend = backend

            if globalmeta.get(concept.name, None):
                raise MetaError('%s already defined' % concept.name)

            globalmeta.add(concept)
            localmeta.add(concept)

        for concept in localmeta('concept'):
            if concept.base:
                concept.base = [localmeta.normalize_name(b) for b in concept.base]

            for link in concept.links.values():
                link.source = concept.name
                link.targets = [localmeta.normalize_name(t) for t in link.targets]


    def order_concepts(self, globalmeta):
        g = {}

        for concept in globalmeta('concept'):
            links = {}
            link_target_types = {}

            for link_name, link in concept.links.items():
                if not isinstance(link.source, meta.GraphObject):
                    link.source = globalmeta.get(link.source)

                targets = set()

                for target in link.targets:
                    if isinstance(target, meta.GraphObject):
                        # Inherited link
                        targets.add(target)
                        continue

                    target_obj = globalmeta.get(target)

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
                            globalmeta.add(atom)

                            targets = {atom}

                        if link.mapping != '11':
                            raise MetaError('%s: links to atoms can only have a "1 to 1" mapping' % link_name)

                        link_target_types[link_name] = 'atom'
                    else:
                        if link_name in link_target_types and link_target_types[link_name] == 'atom':
                            raise MetaError('%s link is already defined as a link to atom')

                        link_target_types[link_name] = 'concept'
                link.targets = targets

            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                g[concept.name]["merge"].extend(concept.base)

        return graph.normalize(g, merger=self.merge_concepts)


    @staticmethod
    def merge_concepts(left, right):
        right.merge(left)
        return right


    @classmethod
    def construct(cls, data, context):
        return cls(data, context)


    def items(self):
        return itertools.chain([('_index_', self.finalindex)], self.finalindex.index_by_name.items())


class Backend(meta.MetaBackend):

    def __init__(self, source_path):
        super().__init__()
        self.metadata = importlib.import_module(ImportContext.construct(source_path))

    def getmeta(self):
        return self.metadata._index_
