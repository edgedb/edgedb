##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy

import importlib
import collections
import itertools

from semantix.utils import graph
from semantix.utils.nlang import morphology

from semantix import caos, lang
from semantix.caos.backends import meta


class MetaError(caos.MetaError):
    def __init__(self, error, context=None):
        super().__init__(error)
        self.context = context

    def __str__(self):
        result = super().__str__()
        if self.context:
            result += '\ncontext: %s, line %d, column %d' % \
                        (self.context.name, self.context.start.line, self.context.start.column)
        return result


class LangObject(lang.meta.Object):
    @classmethod
    def get_canonical_class(cls):
        for base in cls.__bases__:
            if issubclass(base, caos.types.ProtoObject):
                return base

        return cls


class WordCombination(LangObject, morphology.WordCombination):
    def construct(self):
        if isinstance(self.data, str):
            morphology.WordCombination.__init__(self, self.data)
        else:
            word = morphology.WordCombination.from_dict(self.data)
            self.forms = word.forms
            self.value = self.forms.get('singular', next(iter(self.forms.values())))


class AtomModExpr(LangObject, meta.AtomModExpr):
    def construct(self):
        meta.AtomModExpr.__init__(self, self.data['expr'], context=self.context)


class AtomModMinLength(LangObject, meta.AtomModMinLength):
    def construct(self):
        meta.AtomModMinLength.__init__(self, self.data['min-length'], context=self.context)


class AtomModMaxLength(LangObject, meta.AtomModMaxLength):
    def construct(self):
        meta.AtomModMaxLength.__init__(self, self.data['max-length'], context=self.context)


class AtomModRegExp(LangObject, meta.AtomModRegExp):
    def construct(self):
        meta.AtomModRegExp.__init__(self, self.data['regexp'], context=self.context)


class Atom(LangObject, meta.Atom):
    def construct(self):
        data = self.data
        meta.Atom.__init__(self, name=None, backend=data.get('backend'), base=data['extends'],
                           default=data['default'], title=data['title'],
                           description=data['description'])
        mods = data.get('mods')
        if mods:
            for mod in mods:
                self.add_mod(mod)


class Concept(LangObject, meta.Concept):
    def construct(self):
        data = self.data
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        meta.Concept.__init__(self, name=None, backend=data.get('backend'), base=extends,
                              title=data.get('title'), description=data.get('description'))
        self._links = data.get('links', {})


class LinkProperty(LangObject, meta.LinkProperty):
    def construct(self):
        data = self.data
        if isinstance(data, str):
            meta.LinkProperty.__init__(self, name=None, atom=data)
        else:
            atom_name, info = next(iter(data.items()))
            meta.LinkProperty.__init__(self, name=None, atom=atom_name, title=info['title'],
                                       description=info['description'])
            self.mods = info.get('mods')


class LinkDef(LangObject, meta.Link):
    def construct(self):
        data = self.data
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        meta.Link.__init__(self, name=None, backend=data.get('backend'), base=extends, title=data['title'],
                           description=data['description'])
        for property_name, property in data['properties'].items():
            property.name = property_name
            self.add_property(property)


class LinkList(LangObject, list):

    def construct(self):
        data = self.data
        if isinstance(data, str):
            link = meta.Link(source=None, target=data, name=None)
            link.context = self.context
            self.append(link)
        elif isinstance(data, list):
            for target in data:
                link = meta.Link(source=None, target=target, name=None)
                link.context = self.context
                self.append(link)
        else:
            for target, info in data.items():
                link = meta.Link(name=None, target=target, mapping=info['mapping'],
                                 required=info['required'], title=info['title'],
                                 description=info['description'])
                link.mods = info.get('mods')
                link.context = self.context
                self.append(link)


class MetaSet(LangObject):
    def construct(self):
        data = self.data
        context = self.context

        if context.document.import_context.builtin:
            self.include_builtin = True
            realm_meta_class = meta.BuiltinRealmMeta
        else:
            self.include_builtin = False
            realm_meta_class = meta.RealmMeta

        self.finalindex = realm_meta_class()

        self.toplevel = context.document.import_context.toplevel
        globalindex = context.document.import_context.metaindex

        localindex = realm_meta_class()
        localindex.add_module(context.document.module.__name__, None)

        for alias, module in context.document.imports.items():
            localindex.add_module(module.__name__, alias)

        self.read_atoms(data, globalindex, localindex)
        self.read_links(data, globalindex, localindex)
        self.read_concepts(data, globalindex, localindex)

        if self.toplevel:
            # The final pass on concepts may produce additional links and atoms,
            # thus, it has to be performed first.
            concepts = self.order_concepts(globalindex)
            links = self.order_links(globalindex)
            atoms = self.order_atoms(globalindex)

            for atom in atoms:
                if self.include_builtin or atom.name.module != 'semantix.caos.builtins':
                    self.finalindex.add(atom)

            for link in links:
                if self.include_builtin or link.name.module != 'semantix.caos.builtins':
                    self.finalindex.add(link)

            for concept in concepts:
                if self.include_builtin or concept.name.module != 'semantix.caos.builtins':
                    self.finalindex.add(concept)


    def read_atoms(self, data, globalmeta, localmeta):
        backend = data.get('backend')

        for atom_name, atom in data['atoms'].items():
            atom.name = caos.Name(name=atom_name, module=atom.context.document.module.__name__)
            atom.backend = backend
            globalmeta.add(atom)
            localmeta.add(atom)

        for atom in localmeta('atom', include_builtin=self.include_builtin):
            if atom.base:
                try:
                    atom.base = localmeta.normalize_name(atom.base, include_pyobjects=True)
                except caos.MetaError as e:
                    raise MetaError(e, atom.context) from e


    def order_atoms(self, globalmeta):
        g = {}

        for atom in globalmeta('atom', include_automatic=True, include_builtin=self.include_builtin):
            g[atom.name] = {"item": atom, "merge": [], "deps": []}

            if atom.base:
                atom_base = globalmeta.get(atom.base, include_pyobjects=True)
                if isinstance(atom_base, meta.Atom):
                    atom.base = atom_base.name
                    if atom.base.module != 'semantix.caos.builtins':
                        g[atom.name]['deps'].append(atom.base)

        return graph.normalize(g, merger=None)


    def read_links(self, data, globalmeta, localmeta):
        for link_name, link in data['links'].items():
            module = link.context.document.module.__name__
            link.name = caos.Name(name=link_name, module=module)

            properties = {}
            for property_name, property in link.properties.items():
                property.name = caos.Name(name=link_name + '__' + property_name, module=module)
                property.atom = localmeta.normalize_name(property.atom)
                properties[property.name] = property
            link.properties = properties

            globalmeta.add(link)
            localmeta.add(link)

        for link in localmeta('link', include_builtin=self.include_builtin):
            if link.base:
                link.base = [localmeta.normalize_name(b) for b in link.base]
            elif link.name != 'semantix.caos.builtins.link':
                link.base = [caos.Name('semantix.caos.builtins.link')]


    def order_links(self, globalmeta):
        g = {}

        for link in globalmeta('link', include_automatic=True, include_builtin=True):
            for property_name, property in link.properties.items():
                if not isinstance(property.atom, meta.GraphObject):
                    property.atom = globalmeta.get(property.atom)

                    mods = getattr(property, 'mods', None)
                    if mods:
                        # Got an inline atom definition.
                        default = getattr(property, 'default', None)
                        atom = self.genatom(link, property.atom.name, default, property_name, mods)
                        globalmeta.add(atom)
                        property.atom = atom

            if link.source and not isinstance(link.source, meta.GraphObject):
                link.source = globalmeta.get(link.source)

            if link.target and not isinstance(link.target, meta.GraphObject):
                link.target = globalmeta.get(link.target)

            g[link.name] = {"item": link, "merge": [], "deps": []}

            if link.implicit_derivative and not link.atomic():
                base = globalmeta.get(next(iter(link.base)))
                if base.atom:
                    raise caos.MetaError('implicitly defined atomic link % used to link to concept' %
                                         link.name)

            if link.base:
                g[link.name]['merge'].extend(link.base)

        return graph.normalize(g, merger=meta.Link.merge)


    def read_concepts(self, data, globalmeta, localmeta):
        backend = data.get('backend')

        for concept_name, concept in data['concepts'].items():
            concept.name = caos.Name(name=concept_name, module=concept.context.document.module.__name__)
            concept.backend = backend

            if globalmeta.get(concept.name, None):
                raise caos.MetaError('%s already defined' % concept.name)

            globalmeta.add(concept)
            localmeta.add(concept)

        for concept in localmeta('concept', include_builtin=self.include_builtin):
            if concept.base:
                concept.base = [localmeta.normalize_name(b) for b in concept.base]
            elif concept.name != 'semantix.caos.builtins.Object':
                concept.base = [caos.Name('semantix.caos.builtins.Object')]

            for link_name, links in concept._links.items():
                for link in links:
                    link.source = concept.name
                    link.target = localmeta.normalize_name(link.target)

                    link_qname = localmeta.normalize_name(link_name, default=None)
                    if not link_qname:
                        # The link has not been defined globally.
                        if not caos.Name.is_qualified(link_name):
                            # If the name is not fully qualified, assume inline link definition.
                            # The only attribute that is used for global definition is the name.
                            link_qname = caos.Name(name=link_name, module=link.context.document.module.__name__)
                            linkdef = meta.Link(name=link_qname, base=[caos.Name('semantix.caos.builtins.link')])
                            linkdef.atom = globalmeta.get(link.target, type=meta.Atom, default=None) is not None
                            globalmeta.add(linkdef)
                            localmeta.add(linkdef)
                        else:
                            link_qname = caos.Name(link_name)

                    # A new implicit subclass of the link is created for each (source, link_name, target)
                    # combination
                    link.base = {link_qname}
                    link.implicit_derivative = True
                    link_genname = meta.Link.gen_link_name(link.source, link.target, link_qname.name)
                    link.name = caos.Name(name=link_genname, module=link.context.document.module.__name__)
                    globalmeta.add(link)
                    localmeta.add(link)
                    concept.add_link(link)


    def order_concepts(self, globalmeta):
        g = {}

        for concept in globalmeta('concept', include_builtin=True):
            links = {}
            link_target_types = {}

            for link_name, links in concept.links.items():
                for link in links:
                    if not isinstance(link.source, meta.GraphObject):
                        link.source = globalmeta.get(link.source)

                    if not isinstance(link.target, meta.GraphObject):
                        link.target = globalmeta.get(link.target)
                        if isinstance(link.target, caos.types.ProtoConcept):
                            link.target.add_rlink(link)

                    if isinstance(link.target, meta.Atom):
                        if link_name in link_target_types and link_target_types[link_name] != 'atom':
                            raise caos.MetaError('%s link is already defined as a link to non-atom')

                        mods = getattr(link, 'mods', None)
                        if mods:
                            # Got an inline atom definition.
                            default = getattr(link, 'default', None)
                            atom = self.genatom(concept, link.target.name, default, link_name, mods)
                            globalmeta.add(atom)
                            link.target = atom

                        if link.mapping != '11':
                            raise caos.MetaError('%s: links to atoms can only have a "1 to 1" mapping'
                                                 % link_name)

                        link_target_types[link_name] = 'atom'
                    else:
                        if link_name in link_target_types and link_target_types[link_name] == 'atom':
                            raise caos.MetaError('%s link is already defined as a link to atom')

                        link_target_types[link_name] = 'concept'

            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                g[concept.name]["merge"].extend(concept.base)

        return graph.normalize(g, merger=meta.Concept.merge)


    def genatom(self, host, base, default, link_name, mods):
        atom_name = '__' + host.name.name + '__' + link_name.name
        atom = meta.Atom(name=caos.Name(name=atom_name, module=host.name.module),
                         base=base,
                         default=default,
                         automatic=True,
                         backend=host.backend)
        for mod in mods:
            atom.add_mod(mod)
        return atom


    def items(self):
        return itertools.chain([('_index_', self.finalindex)], self.finalindex.index_by_name.items())


class EntityShell(LangObject, caos.concept.EntityShell):
    def __init__(self, data, context):
        super().__init__(data=data, context=context)
        caos.concept.EntityShell.__init__(self)

    def construct(self):
        if isinstance(self.data, str):
            self.id = self.data
        else:
            aliases = {alias: mod.__name__ for alias, mod in self.context.document.imports.items()}
            factory = self.context.document.realm.getfactory(module_aliases=aliases)

            concept, data = next(iter(self.data.items()))
            self.entity = factory(concept)(**data)
            self.context.document.entities.append(self.entity)


class DataSet(LangObject):
    def construct(self):

        entities = {id: [shell.entity for shell in shells] for id, shells in self.data.items()}
        for entity in self.context.document.entities:
            entity.materialize_links(entities)


class Backend(meta.MetaBackend):

    def __init__(self, source_path):
        super().__init__()
        self.metadata = importlib.import_module(meta.ImportContext(source_path, toplevel=True))

    def getmeta(self):
        return self.metadata._index_
