##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import io

import importlib
import collections
import itertools
import decimal

from semantix.utils import lang
from semantix.utils.lang import yaml
from semantix.utils.nlang import morphology
from semantix.utils.algos.persistent_hash import persistent_hash
from semantix.utils.algos import topological

from semantix import caos
from semantix.caos import proto
from semantix.caos import backends
from semantix.caos import delta as base_delta

from . import delta
from .common import StructMeta


class MetaError(caos.MetaError):
    def __init__(self, error, context=None):
        super().__init__(error)
        self.context = context

    def __str__(self):
        result = super().__str__()
        if self.context and self.context.start:
            result += '\ncontext: %s, line %d, column %d' % \
                        (self.context.name, self.context.start.line, self.context.start.column)
        return result


class LangObjectMeta(type(yaml.Object), type(proto.Prototype)):
    def __init__(cls, name, bases, dct, *, adapts=None, ignore_aliases=False):
        type(yaml.Object).__init__(cls, name, bases, dct, adapts=adapts,
                                                          ignore_aliases=ignore_aliases)
        type(proto.Prototype).__init__(cls, name, bases, dct)


class LangObject(yaml.Object, metaclass=LangObjectMeta):
    @classmethod
    def get_canonical_class(cls):
        for base in cls.__bases__:
            if issubclass(base, caos.types.ProtoObject) and not issubclass(base, LangObject):
                return base

        return cls


class WordCombination(LangObject, adapts=morphology.WordCombination):
    def construct(self):
        if isinstance(self.data, str):
            morphology.WordCombination.__init__(self, self.data)
        else:
            word = morphology.WordCombination.from_dict(self.data)
            self.forms = word.forms
            self.value = self.forms.get('singular', next(iter(self.forms.values())))

    @classmethod
    def represent(cls, data):
        return data.as_dict()

    @classmethod
    def adapt(cls, obj):
        return cls.from_dict(obj)


class LinkMapping(LangObject, adapts=caos.types.LinkMapping, ignore_aliases=True):
    def __new__(cls, context, data):
        return caos.types.LinkMapping.__new__(cls, data)

    @classmethod
    def represent(cls, data):
        return str(data)


class PrototypeMeta(LangObjectMeta, StructMeta):
    pass


class Prototype(LangObject, adapts=proto.Prototype, metaclass=PrototypeMeta):
    pass


class AtomMod(LangObject, ignore_aliases=True):
    pass


class AtomModMinLength(AtomMod, adapts=proto.AtomModMinLength):
    def construct(self):
        proto.AtomModMinLength.__init__(self, self.data['min-length'], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'min-length': data.value}


class AtomModMinValue(AtomMod, adapts=proto.AtomModMinValue):
    def construct(self):
        proto.AtomModMinValue.__init__(self, self.data['min-value'], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'min-value': data.value}


class AtomModMinExValue(AtomMod, adapts=proto.AtomModMinExValue):
    def construct(self):
        proto.AtomModMinExValue.__init__(self, self.data['min-value-ex'], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'min-value-ex': data.value}


class AtomModMaxLength(AtomMod, adapts=proto.AtomModMaxLength):
    def construct(self):
        proto.AtomModMaxLength.__init__(self, self.data['max-length'], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'max-length': data.value}


class AtomModMaxValue(AtomMod, adapts=proto.AtomModMaxValue):
    def construct(self):
        proto.AtomModMaxValue.__init__(self, self.data['max-value'], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'max-value': data.value}


class AtomModMaxExValue(AtomMod, adapts=proto.AtomModMaxExValue):
    def construct(self):
        proto.AtomModMaxValue.__init__(self, self.data['max-value-ex'], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'max-value-ex': data.value}


class AtomModPrecision(AtomMod, adapts=proto.AtomModPrecision):
    def construct(self):
        if isinstance(self.data['precision'], int):
            precision = (int(self.data['precision']), 0)
        else:
            precision = int(self.data['precision'][0])
            scale = int(self.data['precision'][1])

            if scale >= precision:
                raise ValueError('Scale must be strictly less than total numeric precision')

            precision = (precision, scale)
        proto.AtomModPrecision.__init__(self, precision, context=self.context)

    @classmethod
    def represent(cls, data):
        if data.value[1] is None:
            return {'precision': data.value[0]}
        else:
            return {'precision': list(data.value)}


class AtomModRounding(AtomMod, adapts=proto.AtomModRounding):
    map = {
        'ceiling': decimal.ROUND_CEILING,
        'down': decimal.ROUND_DOWN,
        'floor': decimal.ROUND_FLOOR,
        'half-down': decimal.ROUND_HALF_DOWN,
        'half-even': decimal.ROUND_HALF_EVEN,
        'half-up': decimal.ROUND_HALF_UP,
        'up': decimal.ROUND_UP,
        '05up': decimal.ROUND_05UP
    }

    rmap = dict(zip(map.values(), map.keys()))

    def construct(self):
        proto.AtomModRounding.__init__(self, self.map[self.data['rounding']], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'rounding': cls.rmap[data.value]}


class AtomModExpr(AtomMod, adapts=proto.AtomModExpr):
    def construct(self):
        proto.AtomModExpr.__init__(self, [self.data['expr'].strip(' \n')], context=self.context)

    @classmethod
    def represent(cls, data):
        return {'expr': next(iter(data.values))}


class AtomModRegExp(AtomMod, adapts=proto.AtomModRegExp):
    def construct(self):
        proto.AtomModRegExp.__init__(self, [self.data['regexp']], context=self.context)

    @classmethod
    def represent(self, data):
        return {'regexp': next(iter(data.values))}

default_name = caos.Name('!unknown.name')

class Atom(Prototype, adapts=proto.Atom):
    def construct(self):
        data = self.data
        proto.Atom.__init__(self, name=default_name, backend=None, base=data['extends'],
                            default=data['default'], title=data['title'],
                            description=data['description'], is_abstract=data['abstract'],
                            _setdefaults_=False)
        mods = data.get('mods')
        if mods:
            for mod in mods:
                self.add_mod(mod)

    @classmethod
    def represent(cls, data):
        result = {
            'extends': data.base
        }

        if data.base:
            result['extends'] = data.base

        if data.default is not None:
            result['default'] = data.default

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.is_abstract:
            result['abstract'] = data.is_abstract

        if data.mods:
            result['mods'] = list(itertools.chain.from_iterable(data.mods.values()))

        return result


class Concept(Prototype, adapts=proto.Concept):
    def construct(self):
        data = self.data
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.Concept.__init__(self, name=default_name, backend=None,
                               base=tuple(extends) if extends else tuple(),
                               title=data.get('title'), description=data.get('description'),
                               is_abstract=data.get('abstract'),
                               _setdefaults_=False)
        self._links = data.get('links', {})

    @classmethod
    def represent(cls, data):
        result = {
            'extends': list(itertools.chain(data.base, data.custombases))
        }

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.is_abstract:
            result['abstract'] = data.is_abstract

        if data.ownlinks:
            result['links'] = dict(data.ownlinks)

        return result


class LinkProperty(Prototype, adapts=proto.LinkProperty, ignore_aliases=True):
    def construct(self):
        data = self.data
        if isinstance(data, str):
            proto.LinkProperty.__init__(self, name=default_name, atom=data)
        else:
            atom_name, info = next(iter(data.items()))
            proto.LinkProperty.__init__(self, name=default_name, atom=atom_name, title=info['title'],
                                       description=info['description'])
            self.mods = info.get('mods')

    @classmethod
    def represent(cls, data):
        result = {}

        if data.atom.mods and data.atom.automatic:
            result['mods'] = list(itertools.chain.from_iterable(data.atom.mods.values()))

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if result:
            return {data.atom.name: result}
        else:
            return str(data.atom.name)


class LinkDef(Prototype, adapts=proto.Link):
    def construct(self):
        data = self.data
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.Link.__init__(self, name=default_name, backend=None,
                            base=tuple(extends) if extends else tuple(),
                            title=data['title'], description=data['description'],
                            is_abstract=data.get('abstract'),
                            readonly=data.get('readonly'),
                            _setdefaults_=False)
        for property_name, property in data['properties'].items():
            property.name = property_name
            self.add_property(property)

    @classmethod
    def represent(cls, data):
        result = {}

        if not data.implicit_derivative:
            if data.base:
                result['extends'] = list(data.base)

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.is_abstract:
            result['abstract'] = data.is_abstract

        if data.readonly:
            result['readonly'] = data.readonly

        if data.mapping:
            result['mapping'] = data.mapping

        if isinstance(data.target, proto.Atom) and data.target.automatic:
            result['mods'] = list(itertools.chain.from_iterable(data.target.mods.values()))

        if data.required:
            result['required'] = data.required

        if data.properties:
            result['properties'] = data.properties

        if data.constraints:
            constraints = itertools.chain.from_iterable(data.constraints.values())
            result['constraints'] = list(constraints)

        return result


class LinkSet(Prototype, adapts=proto.LinkSet):
    @classmethod
    def represent(cls, data):
        result = {}

        for l in data.links:
            if isinstance(l.target, proto.Atom) and l.target.automatic:
                key = l.target.base
            else:
                key = l.target.name
            result[str(key)] = l

        return result


class LinkConstraint(Prototype, adapts=proto.LinkConstraint, ignore_aliases=True):
    @classmethod
    def represent(cls, data):
        return {cls.constraint_name: next(iter(data.values))}


class LinkConstraintUnique(LinkConstraint, adapts=proto.LinkConstraintUnique):
    def construct(self):
        values = {self.data[self.__class__.constraint_name]}
        proto.LinkConstraintUnique.__init__(self, values, context=self.context)


class LinkList(LangObject, list):

    def construct(self):
        data = self.data
        if isinstance(data, str):
            link = proto.Link(source=None, target=data, name=default_name, _setdefaults_=False)
            link.context = self.context
            self.append(link)
        elif isinstance(data, list):
            for target in data:
                link = proto.Link(source=None, target=target, name=default_name, _setdefaults_=False)
                link.context = self.context
                self.append(link)
        else:
            for target, info in data.items():
                if not isinstance(target, tuple):
                    target = (target,)

                for t in target:
                    link = proto.Link(name=default_name, target=t, mapping=info['mapping'],
                                      required=info['required'], title=info['title'],
                                      description=info['description'], readonly=info['readonly'],
                                      _setdefaults_=False)
                    link.mods = info.get('mods')
                    link.context = self.context

                    constraints = info.get('constraints')
                    if constraints:
                        for constraint in constraints:
                            link.add_constraint(constraint)

                    self.append(link)


class MetaSet(LangObject):
    def construct(self):
        data = self.data
        context = self.context

        if context.document.import_context.builtin:
            self.include_builtin = True
            realm_meta_class = proto.BuiltinRealmMeta
        else:
            self.include_builtin = False
            realm_meta_class = proto.RealmMeta

        self.toplevel = context.document.import_context.toplevel
        globalindex = context.document.import_context.metaindex

        localindex = realm_meta_class()
        self.module = data.get('module', None)
        if not self.module:
            self.module = context.document.module.__name__
        localindex.add_module(self.module, None)

        if self.toplevel and self.module and caos.Name.is_qualified(self.module):
            main_module = caos.Name(self.module)
        else:
            main_module = None
        self.finalindex = realm_meta_class(main_module=main_module)

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
                    atom.setdefaults()
                    self.finalindex.add(atom)

            for link in links:
                if self.include_builtin or link.name.module != 'semantix.caos.builtins':
                    link.setdefaults()
                    self.finalindex.add(link)

            for concept in concepts:
                if self.include_builtin or concept.name.module != 'semantix.caos.builtins':
                    concept.setdefaults()
                    self.finalindex.add(concept)


    def read_atoms(self, data, globalmeta, localmeta):
        backend = None

        for atom_name, atom in data['atoms'].items():
            atom.name = caos.Name(name=atom_name, module=self.module)
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

        for atom in globalmeta('atom', include_automatic=True, include_builtin=True):
            g[atom.name] = {"item": atom, "merge": [], "deps": []}

            if atom.base:
                atom_base = globalmeta.get(atom.base, include_pyobjects=True)
                if isinstance(atom_base, proto.Atom):
                    atom.base = atom_base.name
                    g[atom.name]['merge'].append(atom.base)

        return topological.normalize(g, merger=proto.Atom.merge)


    def read_links(self, data, globalmeta, localmeta):
        for link_name, link in data['links'].items():
            module = self.module
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
                link.base = tuple(localmeta.normalize_name(b) for b in link.base)
            elif link.name != 'semantix.caos.builtins.link':
                link.base = (caos.Name('semantix.caos.builtins.link'),)


    def order_links(self, globalmeta):
        g = {}

        for link in globalmeta('link', include_automatic=True, include_builtin=True):
            for property_name, property in link.properties.items():
                if not isinstance(property.atom, proto.Prototype):
                    property.atom = globalmeta.get(property.atom)

                    mods = getattr(property, 'mods', None)
                    if mods:
                        # Got an inline atom definition.
                        default = getattr(property, 'default', None)
                        atom = self.genatom(link, property.atom.name, default, property_name, mods)
                        globalmeta.add(atom)
                        property.atom = atom

            if link.source and not isinstance(link.source, proto.Prototype):
                link.source = globalmeta.get(link.source)

            if link.target and not isinstance(link.target, proto.Prototype):
                link.target = globalmeta.get(link.target)

            g[link.name] = {"item": link, "merge": [], "deps": []}

            if link.implicit_derivative and not link.atomic():
                base = globalmeta.get(next(iter(link.base)))
                if base.is_atom:
                    raise caos.MetaError('implicitly defined atomic link %s used to link to concept'
                                          % link.name)

            if link.base:
                g[link.name]['merge'].extend(link.base)

        return topological.normalize(g, merger=proto.Link.merge)


    def read_concepts(self, data, globalmeta, localmeta):
        backend = None

        for concept_name, concept in data['concepts'].items():
            concept.name = caos.Name(name=concept_name, module=self.module)
            concept.backend = backend

            if globalmeta.get(concept.name, None):
                raise caos.MetaError('%s already defined' % concept.name)

            globalmeta.add(concept)
            localmeta.add(concept)

        for concept in localmeta('concept', include_builtin=self.include_builtin):
            bases = []
            custombases = []

            if concept.base:
                for b in concept.base:
                    base_name = localmeta.normalize_name(b, include_pyobjects=True)
                    if proto.Concept.is_prototype(base_name):
                        bases.append(base_name)
                    else:
                        cls = localmeta.get(base_name, include_pyobjects=True)
                        if not issubclass(cls, caos.concept.Concept):
                            raise caos.MetaError('custom concept base classes must inherit from '
                                                 'caos.concept.Concept: %s' % base_name)
                        custombases.append(base_name)

            if not bases and concept.name != 'semantix.caos.builtins.BaseObject':
                bases.append(caos.Name('semantix.caos.builtins.Object'))

            concept.base = tuple(bases)
            concept.custombases = tuple(custombases)

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
                            link_qname = caos.Name(name=link_name, module=self.module)
                            linkdef = proto.Link(name=link_qname,
                                                 base=(caos.Name('semantix.caos.builtins.link'),))
                            target_atom = globalmeta.get(link.target, type=proto.Atom, default=None)
                            linkdef.is_atom = target_atom is not None
                            globalmeta.add(linkdef)
                            localmeta.add(linkdef)
                        else:
                            link_qname = caos.Name(link_name)

                    # A new implicit subclass of the link is created for each
                    # (source, link_name, target) combination
                    link.base = (link_qname,)
                    link.implicit_derivative = True
                    link_genname = proto.Link.gen_link_name(link.source, link.target, link_qname.name)
                    link.name = caos.Name(name=link_genname, module=link_qname.module)
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
                    if not isinstance(link.source, proto.Prototype):
                        link.source = globalmeta.get(link.source)

                    if not isinstance(link.target, proto.Prototype):
                        link.target = globalmeta.get(link.target)
                        if isinstance(link.target, caos.types.ProtoConcept):
                            link.target.add_rlink(link)

                    if isinstance(link.target, proto.Atom):
                        link.is_atom = True

                        if link_name in link_target_types and link_target_types[link_name] != 'atom':
                            raise caos.MetaError('%s link is already defined as a link to non-atom')

                        mods = getattr(link, 'mods', None)
                        if mods:
                            # Got an inline atom definition.
                            default = getattr(link, 'default', None)
                            atom = self.genatom(concept, link.target.name, default, link_name, mods)
                            globalmeta.add(atom)
                            link.target = atom

                        if link.mapping and link.mapping != caos.types.OneToOne:
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

        return topological.normalize(g, merger=proto.Concept.merge)


    def genatom(self, host, base, default, link_name, mods):
        atom_name = Atom.gen_atom_name(host, link_name)
        atom = proto.Atom(name=caos.Name(name=atom_name, module=host.name.module),
                          base=base, default=default, automatic=True, backend=None)
        for mod in mods:
            atom.add_mod(mod)
        return atom


    def items(self):
        return itertools.chain([('_index_', self.finalindex), ('_module_', self.module)],
                               self.finalindex.index_by_name.items())


class EntityShell(LangObject, adapts=caos.concept.EntityShell):
    def __init__(self, data, context):
        super().__init__(data=data, context=context)
        caos.concept.EntityShell.__init__(self)

    def construct(self):
        if isinstance(self.data, str):
            self.id = self.data
        else:
            aliases = {alias: mod.__name__ for alias, mod in self.context.document.imports.items()}
            session = self.context.document.session
            factory = session.realm.getfactory(module_aliases=aliases, session=session)

            concept, data = next(iter(self.data.items()))
            self.entity = factory(concept)(**data)
            self.context.document.entities.append(self.entity)


class RealmMeta(LangObject, adapts=proto.RealmMeta):
    @classmethod
    def represent(cls, data):
        result = {'atoms': {}, 'links': {}, 'concepts': {}}

        for type in ('atom', 'link', 'concept'):
            for obj in data(type=type, include_builtin=False, include_automatic=False):
                # XXX
                if type == 'link' and obj.implicit_derivative:
                    continue
                result[type + 's'][str(obj.name)] = obj

        return result


class DataSet(LangObject):
    def construct(self):

        entities = {id: [shell.entity for shell in shells] for id, shells in self.data.items()}
        for entity in self.context.document.entities:
            entity.__class__.materialize_links(entity, entities)


class CaosName(LangObject, adapts=caos.Name, ignore_aliases=True):
    @classmethod
    def represent(cls, data):
        return str(data)

    def construct(self):
        caos.Name.__init__(self, self.data)


class ModuleFromData:
    def __init__(self, name):
        self.__name__ = name


class Backend(backends.MetaBackend):

    def __init__(self, deltarepo, module=None, data=None):
        if module:
            self.metadata = module
        else:
            self.metadata = self.load_from_string(data)

        modhash = persistent_hash(self.metadata._module_)

        repo = deltarepo(module=self.metadata._module_, id=modhash)
        super().__init__(repo)

    def load_from_string(self, data):
        import_context = proto.ImportContext('<string>', toplevel=True)
        module = ModuleFromData('<string>')
        context = lang.meta.DocumentContext(module=module, import_context=import_context)
        for k, v in lang.yaml.Language.load_dict(io.StringIO(data), context):
            setattr(module, str(k), v)

        return module

    def getmeta(self):
        return self.metadata._index_

    def dump_meta(self, meta):
        prologue = '%SCHEMA semantix.caos.backends.yaml.schemas.Semantics\n---\n'
        return prologue + yaml.Language.dump(meta)
