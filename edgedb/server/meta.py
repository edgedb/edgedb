##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import collections
import hashlib
import importlib

from semantix import caos, lang
from semantix.utils.datastructures import OrderedSet

class MetaBackend(object):
    def getmeta(self):
        pass


class GraphObjectBackendData:
    pass


class GraphObjectBackendDataContainer:
    def __getattr__(self, name):
        result = GraphObjectBackendData()
        setattr(self, name, result)
        return result


class GraphObject(caos.types.ProtoObject):
    def __init__(self, name, backend=None, base=None, title=None, description=None):
        self.name = name
        self.base = base
        self.title = title
        self.description = description
        self.backend = backend
        self.backend_data = GraphObjectBackendDataContainer()
        self.children = set()

    def get_class_template(self, realm):
        """
            Return a tuple (bases, classdict) to be used to
            construct a class representing the graph object
        """
        bases = self.get_class_base(realm)

        metadata = caos.types.GraphObjectMetaData()
        metadata.realm = realm
        metadata.name = self.name
        metadata.backend = self.backend
        metadata.prototype = self
        dct = {'_metadata': metadata, '__module__': self.name.module}

        name = self.get_class_name(realm)

        if self.title:
            dct['_title'] = self.title

        if self.description:
            dct['_description'] = self.description

        return name, bases, dct, type

    def get_class_base(self, realm):
        if isinstance(self.base, caos.Name):
            base = (realm.meta.get(self.base),)
        else:
            base = self.base

        return base

    def get_class_name(self, realm):
        return '%s_%s' % (self.__class__.__name__, self.name.name)

    def get_prototype(self, realm, name):
        if isinstance(name, caos.Name):
            return realm.meta.get(name, include_pyobjects=True)
        else:
            return name

    def _merge_mro(self, mros):
        result = []

        while True:
            nonempty = [mro for mro in mros if mro]
            if not nonempty:
                return result

            for mro in nonempty:
                candidate = mro[0]
                tails = [m for m in nonempty if candidate in m[1:]]
                if not tails:
                    break
            else:
                raise caos.types.MetaError("Could not find consistent MRO for %s" % self.name)

            result.append(candidate)

            for mro in nonempty:
                if mro[0] == candidate:
                    del mro[0]

        return result

    def get_mro(self, realm):
        bases = self.get_class_base(realm)
        mros = [[self]]
        for base in bases:
            mros.append(base.get_mro(realm))
        return self._merge_mro(mros)

    def issubclass(self, realm, parent):
        if isinstance(parent, caos.types.ProtoObject):
            return parent in self.get_mro(realm)
        else:
            return bool(set(parent) & set(self.get_mro(realm)))

    def merge(self, obj):
        if not isinstance(obj, self.__class__):
            raise caos.types.MetaMismatchError("cannot merge instances of %s and %s" % (obj.__class__.__name__, self.__class__.__name__))

    def __repr__(self):
        return '<caos.%sPrototype %s>' % (self.__class__.__name__, self.name)


class AtomMod(caos.types.ProtoObject):
    def __init__(self, context):
        self.context = context

    def merge(self, mod):
        if not isinstance(mod, self.__class__):
            raise caos.types.MetaMismatchError("cannot merge instances of %s and %s" % (mod.__class__.__name__, self.__class__.__name__))

    def validate(self, value):
        pass


class AtomModExpr(AtomMod):
    def __init__(self, expr, context=None):
        super().__init__(context)
        self.exprs = [expr]

    def merge(self, mod):
        super().merge(mod)
        self.exprs.extend(mod.exprs)

    def validate(self, value):
        for expr in self.exprs:
            e = expr.replace('VALUE', repr(value))
            result = eval(e)
            if not result:
                raise ValueError('constraint violation: %s is not True' % e)

    def __str__(self):
        return '<%s: %s>' % (self.__class__.__name__, ', '.join(self.exprs))


class AtomModMinLength(AtomMod):
    def __init__(self, value, context=None):
        super().__init__(context)
        self.value = value

    def merge(self, mod):
        super().merge(mod)
        self.value = max(self.value, mod.value)

    def validate(self, value):
        if len(str(value)) < self.value:
            raise ValueError('constraint violation: %r length is less than required %d' % (value, self.value))

    def __str__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.value)


class AtomModMaxLength(AtomMod):
    def __init__(self, value, context=None):
        super().__init__(context)
        self.value = value

    def merge(self, mod):
        super().merge(mod)
        self.value = min(self.value, mod.value)

    def validate(self, value):
        if len(str(value)) > self.value:
            raise ValueError('constraint violation: %r length is more than allowed %d' % (value, self.value))

    def __str__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.value)


class AtomModRegExp(AtomMod):
    def __init__(self, regexp, context=None):
        super().__init__(context)
        self.regexps = {regexp.strip(): re.compile(regexp.strip())}

    def merge(self, mod):
        super().merge(mod)
        self.regexps.update(mod.regexps)

    def validate(self, value):
        for regexp_text, regexp in self.regexps.items():
            if not regexp.match(value):
                raise ValueError('constraint violation: %r does not match regular expression "%s"' % (value, regexp_text))

    def __str__(self):
        return '<%s: %s>' % (self.__class__.__name__, ', '.join(self.regexps.keys()))


class Atom(GraphObject, caos.types.ProtoAtom):
    _type = 'atom'

    def __init__(self, name, backend=None, base=None, title=None, description=None,
                 default=None, automatic=False):
        super().__init__(name, backend, base, title, description)
        self.mods = {}
        self.default = default
        self.automatic = automatic

    def add_mod(self, mod):
        if mod.__class__ not in self.mods:
            self.mods[mod.__class__] = mod
        else:
            self.mods[mod.__class__].merge(mod)

    def get_class_template(self, realm):
        name, bases, dct, metaclass = super().get_class_template(realm)

        base = self.get_prototype(realm, self.base)

        dct.update({'mods': self.mods, 'base': base, 'default': self.default})

        metaclass = caos.atom.AtomMeta
        return (name, bases, dct, metaclass)

    def get_class_base(self, realm):
        bases = tuple()

        if self.base:
            base = self.get_prototype(realm, self.base)
            bases = (base,)

        return bases


class Concept(GraphObject, caos.types.ProtoConcept):
    _type = 'concept'

    def __init__(self, name, backend=None, base=None, title=None, description=None):
        super().__init__(name, backend, base, title, description)

        self.ownlinks = {}
        self.links = {}
        self.rlinks = set()

    def merge(self, other):
        super().merge(other)

        other.children.update(self.children, {self})
        self.links.update(other.links)
        self.rlinks.update(other.rlinks)

    def add_link(self, link):
        if link.implicit_derivative:
            key = next(iter(link.base))
        else:
            key = link.name
        links = self.links.get(key)
        if links:
            links.add(link)
        else:
            self.links[key] = LinkSet(links=[link], name=key, source=link.source)
            self.ownlinks[key] = self.links[key]

    def add_rlink(self, link):
        self.rlinks.add(link)

    def get_class_template(self, realm):
        name, bases, dct, metaclass = super().get_class_template(realm)
        dct['_metadata'].links = self.links
        dct['_metadata'].ownlinks = self.ownlinks
        dct['_metadata'].link_map = {}
        dct['_metadata'].link_rmap = {}

        metaclass = caos.concept.ConceptMeta
        return (name, bases, dct, metaclass)

    def get_class_base(self, realm):
        bases = tuple()
        if self.base:
            for parent in self.base:
                bases += (realm.meta.get(parent),)

        return bases

    def __str__(self):
        return '%s' % self.name


class LinkSet(GraphObject):
    def __init__(self, name, source, links):
        super().__init__(name=name)
        self.links = set(links)
        self.name = name
        self.source = source
        self.ownlinks = {}

    def add(self, link):
        self.links.add(link)

    def atomic(self):
        return len(self.links) == 1 and self.first.atomic()

    def singular(self):
        return len(self.links) == 1 and self.first.mapping == '11'

    @property
    def first(self):
        return next(iter(self.links))

    def get_class_template(self, realm):
        name, bases, dct, metaclass = super().get_class_template(realm)

        name += '_set'

        dct['_metadata'].links = self.links
        dct['_metadata'].link_name = self.name
        dct['_metadata'].source = self.source
        metaclass = caos.link.LinkSetMeta

        return (name, bases, dct, metaclass)

    def get_class_base(self, realm):
        bases = tuple()
        bases += (caos.link.LinkSet,)
        return bases

    def __iter__(self):
        return iter(self.links)


class LinkProperty(GraphObject):
    def __init__(self, name, atom, backend=None, title=None, description=None):
        super().__init__(name, backend=backend, title=title, description=description)
        self.atom = atom
        self.mods = {}


class Link(GraphObject, caos.types.ProtoLink):
    _type = 'link'

    def __init__(self, name, backend=None, base=None, title=None, description=None,
                 source=None, target=None, mapping='11', required=False, implicit_derivative=False):
        super().__init__(name, backend, base, title=title, description=description)
        self.source = source
        self.target = target
        self.mapping = mapping
        self.required = required
        self.implicit_derivative = implicit_derivative
        self.properties = {}
        self.atom = False

    def merge(self, other):
        self.properties.update(other.properties)

    def add_property(self, property):
        self.properties[property.name] = property

    def get_class_template(self, realm):
        name, bases, dct, metaclass = super().get_class_template(realm)
        dct.update({'required': self.required})

        dct['_metadata'].link_name = self.name
        dct['_metadata'].protosource = self.source
        dct['_metadata'].prototarget = self.target
        dct['_metadata'].required = self.required

        if self.implicit_derivative:
            dct['_metadata'].prototype = realm.meta.get(next(iter(self.base)))

        metaclass = caos.link.LinkMeta

        if self.mapping:
            dct['mapping'] = self.mapping

        if self.properties:
            dct['properties'] = self.properties

        return (name, bases, dct, metaclass)

    def get_class_base(self, realm):
        bases = tuple()
        if self.base:
            for parent in self.base:
                bases += (realm.meta.get(parent),)

        return bases

    @classmethod
    def gen_link_name(cls, source, target, basename):
        # XXX: Determine if it makes sense to use human-generatable name here
        hash = hashlib.md5(str(source).encode() + str(target).encode()).hexdigest()
        return '%s_%s' % (basename, hash)

    def atomic(self):
        return (self.target and isinstance(self.target, Atom)) or self.atom


class ImportContext(lang.ImportContext):
    def __new__(cls, name, *, metaindex=None, toplevel=False, builtin=False):
        result = super(ImportContext, cls).__new__(cls, name)
        result.metaindex = metaindex if metaindex else (BuiltinRealmMeta() if builtin else RealmMeta())
        result.metaindex.add_module(name, name)
        result.toplevel = toplevel
        result.builtin = builtin
        return result

    def __init__(self, name, *, metaindex=None, toplevel=False, builtin=False):
        pass

    @classmethod
    def from_parent(cls, name, parent):
        if parent and isinstance(parent, ImportContext):
            result = cls(name, metaindex=parent.metaindex, toplevel=False, builtin=parent.builtin)
            result.metaindex.add_module(name, name)
        else:
            result = cls(name, toplevel=True)
        return result

    @classmethod
    def copy(cls, name, other):
        if isinstance(other, ImportContext):
            result = cls(other, metaindex=other.metaindex, toplevel=other.toplevel, builtin=other.builtin)
        else:
            result = cls(other)
        return result


class RealmMetaIterator(object):
    def __init__(self, index, type, include_automatic=False, include_builtin=False):
        self.index = index
        self.type = type

        sourceset = self.index.index

        if type is not None:
            itertype = index.index_by_type[type]

            if sourceset:
                sourceset = itertype & sourceset
            else:
                sourceset = itertype

        filtered = sourceset
        if not include_builtin:
            filtered = filtered - index.index_builtin
        if not include_automatic:
            filtered = filtered - index.index_automatic
        self.iter = iter(filtered)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iter)


class RealmMeta(object):

    def __init__(self):
        # XXX: TODO: refactor this ugly index explosion into a single smart and efficient one
        self.index = OrderedSet()
        self.index_by_type = {'concept': OrderedSet(), 'atom': OrderedSet(), 'link': OrderedSet()}
        self.index_by_name = collections.OrderedDict()
        self.index_by_module = collections.OrderedDict()
        self.index_builtin = OrderedSet()
        self.index_automatic = OrderedSet()
        self.index_by_backend = {}
        self.modules = {}
        self.rmodules = set()

        self._init_builtin()

    def add(self, obj):
        if obj.name in self.index_by_name:
            raise caos.MetaError('object named "%s" is already present in the meta index' % obj.name)

        self.index.add(obj)
        type = obj._type
        self.index_by_type[type].add(obj)

        if obj.name.module not in self.index_by_module:
            self.index_by_module[obj.name.module] = collections.OrderedDict()
        self.index_by_module[obj.name.module][obj.name.name] = obj
        self.index_by_name[obj.name] = obj
        if obj.name.module.startswith('caos'):
            raise Exception('mangled name passed %s' % obj.name)
        if obj.backend:
            self.index_by_backend[obj.backend] = obj

        if obj.name.module == 'semantix.caos.builtins':
            self.index_builtin.add(obj)

        if isinstance(obj, Atom):
            if obj.automatic:
                self.index_automatic.add(obj)

    def add_module(self, module, alias):
        existing = self.modules.get(alias)
        if existing and existing != module:
            raise caos.MetaError('Alias %s is already bound to module %s' % (alias, self.modules[alias]))
        else:
            self.modules[alias] = module
            self.rmodules.add(module)

    def get(self, name, default=caos.MetaError, module_aliases=None, type=None, include_pyobjects=False):
        obj = self.lookup_name(name, module_aliases, default=None)

        if not obj and include_pyobjects:
            module_name, attrname = str(name).rpartition('.')[::2]
            if module_name:
                try:
                    obj = getattr(importlib.import_module(module_name), attrname)
                except (ImportError, AttributeError):
                    pass

        if not obj:
            if default and issubclass(default, caos.MetaError):
                raise default('reference to a non-existent semantic graph node: %s' % name)
            else:
                obj = default
        if type and not isinstance(obj, type):
            if default and issubclass(default, caos.MetaError):
                raise default('reference to a non-existent %s %s' %
                              (type.__name__, name))
            else:
                obj = default
        return obj

    def match(self, name, module_aliases=None, type=None):
        name, module, nqname = self._split_name(name)

        result = []

        if '%' in nqname:
            module = self.resolve_module(module, module_aliases)
            if not module:
                return None

            pattern = re.compile(re.escape(nqname).replace('\%', '.*'))
            index = self.index_by_module.get(module)

            for name, obj in index.items():
                if pattern.match(name):
                    if type and isinstance(obj, type):
                        result.append(obj)
        else:
            result = self.get(name, module_aliases=module_aliases, type=type, default=None)
            if result:
                result = [result]

        return result

    def backends(self):
        return list(self.index_by_backend.keys())

    def __iter__(self):
        return RealmMetaIterator(self, None)

    def __call__(self, type=None, include_automatic=False, include_builtin=False):
        return RealmMetaIterator(self, type, include_automatic, include_builtin)

    def __contains__(self, obj):
        return obj in self.index

    def normalize_name(self, name, module_aliases=None, default=caos.MetaError, include_pyobjects=False):
        name, module, nqname = self._split_name(name)
        norm_name = None

        if module is None:
            object = None
            default_module = self.resolve_module(module, module_aliases)

            if default_module:
                object = self.lookup_qname(caos.Name(name=nqname, module=default_module))
            if not object:
                object = self.lookup_qname(caos.Name(name=nqname, module='semantix.caos.builtins'))
            if object:
                norm_name = object.name
        else:
            object = self.lookup_qname(name)

            if object:
                norm_name = object.name
            else:
                fullmodule = self.resolve_module(module, module_aliases)
                if not fullmodule:
                    if module in self.rmodules or (module_aliases and module in module_aliases.values()):
                        fullmodule = module

                if fullmodule:
                    norm_name = caos.Name(name=nqname, module=fullmodule)

        if not norm_name and include_pyobjects and module:
            try:
                getattr(importlib.import_module(module), nqname)
                norm_name = name
            except (ImportError, AttributeError):
                pass

        if norm_name:
            return norm_name
        else:
            if default and issubclass(default, caos.MetaError):
                raise default('could not normalize caos name %s' % name)
            else:
                return default

    def resolve_module(self, module, module_aliases):
        if module_aliases:
            module = module_aliases.get(module, self.modules.get(module))
        else:
            module = self.modules.get(module)
        return module

    def lookup_name(self, name, module_aliases=None, default=caos.MetaError):
        name = self.normalize_name(name, module_aliases, default=default)
        if name:
            return self.lookup_qname(name)

    def lookup_qname(self, name):
        module = self.index_by_module.get(name.module)
        if module:
            return module.get(name.name)

    def _init_builtin(self):
        module = ImportContext(name='semantix.caos.builtins', toplevel=True, builtin=True)
        builtins = importlib.import_module(module)
        for obj in builtins._index_.index:
            self.add(obj)

    def _split_name(self, name):
        if isinstance(name, caos.Name):
            module = name.module
            nqname = name.name
        elif isinstance(name, tuple):
            module = name[0]
            nqname = name[1]
            name = module + '.' + nqname if module else nqname
        elif caos.Name.is_qualified(name):
            name = caos.Name(name)
            module = name.module
            nqname = name.name
        else:
            module = None
            nqname = name

        return name, module, nqname


class BuiltinRealmMeta(RealmMeta):
    def _init_builtin(self):
        pass
