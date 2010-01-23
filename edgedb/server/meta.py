import re
import collections
import hashlib

import semantix.caos.atom
import semantix.caos.concept
import semantix.caos.link
from semantix.caos.name import Name as CaosName
from semantix.utils.datastructures import OrderedSet


class MetaError(Exception):
    pass


class MetaMismatchError(Exception):
    pass


class MetaBackend(object):
    def getmeta(self):
        pass


class Bool(type):
    def __init__(self, value):
        self.value = value


class MetaObject(object):
    @classmethod
    def get_canonical_class(cls):
        return cls


class GraphObject(MetaObject):
    def __init__(self, name, backend=None, base=None, title=None, description=None):
        self.name = name
        self.base = base
        self.title = title
        self.description = description
        self.backend = backend

    def get_class_template(self, realm):
        """
            Return a tuple (bases, classdict) to be used to
            construct a class representing the graph object
        """
        bases = self.get_class_base(realm)
        dct = {'name': self.name, 'realm': realm, 'backend': self.backend, '__module__': self.name.module}

        name = self.get_class_name(realm)

        if self.title:
            dct['title'] = self.title

        if self.description:
            dct['description'] = self.description

        return name, bases, dct

    def get_class_base(self, realm):
        if isinstance(self.base, CaosName):
            base = (realm.getfactory()(self.base),)
        else:
            base = self.base

        return base

    def get_class_name(self, realm):
        return '%s_%s' % (self.__class__.__name__, self.name.name)

    def merge(self, obj):
        if not isinstance(obj, self.__class__):
            raise MetaMismatchError("cannot merge instances of %s and %s" % (obj.__class__.__name__, self.__class__.__name__))


class Node(GraphObject):
    pass


class AtomMod(MetaObject):
    def __init__(self, context):
        self.context = context

    def merge(self, mod):
        if not isinstance(mod, self.__class__):
            raise MetaMismatchError("cannot merge instances of %s and %s" % (mod.__class__.__name__, self.__class__.__name__))

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


class Atom(Node):
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
        name, bases, dct = super().get_class_template(realm)

        base = bases[0] if bases else None

        default = self.get_class_default(realm, base)

        dct.update({'mods': self.mods, 'base': base, 'default': default})

        bases = self.get_class_mro(realm, base)
        return (name, bases, dct)

    def get_class_mro(self, realm, clsbase):
        bases = tuple()

        if self.base:
            if isinstance(self.base, CaosName):
                base = realm.meta.get(self.base)
            else:
                base = self.base

            bases = base.get_class_mro(realm, clsbase)
            if clsbase not in bases:
                bases = (clsbase,) + bases
        else:
            bases = (semantix.caos.atom.Atom,)

        return bases

    def get_class_default(self, realm, base):
        if self.default is not None:
            return base(self.default)


class BuiltinAtom(Atom):
    base_atoms_to_class_map = {
                                'str': str,
                                'int': int,
                                'float': float,
                                'bool': Bool
                              }

    def get_class_mro(self, realm, clsbase):
        base = self.base_atoms_to_class_map[self.name.name]
        bases = (semantix.caos.atom.Atom, base)
        return bases


class Concept(Node):
    _type = 'concept'

    def __init__(self, name, backend=None, base=None, title=None, description=None):
        super().__init__(name, backend, base, title, description)

        self.links = {}

    def merge(self, other):
        super().merge(other)

        self.links.update(other.links)

    def add_link(self, link):
        if link.implicit_derivative:
            key = next(iter(link.base))
        else:
            key = link.name
        links = self.links.get(key)
        if links:
            links.add(link)
        else:
            self.links[key] = semantix.caos.link.LinkSet([link], name=key)

    def get_class_template(self, realm):
        name, bases, dct = super().get_class_template(realm)
        dct.update({'concept': self.name, 'links': self.links, 'parents': self.base})

        # XXX: static concept class attributes may mask "real" entity attributes
        # since those are now set using fully qualified name and Concept::__getattr__
        # does not get triggered.
        del dct['name']

        return (name, bases, dct)

    def get_class_base(self, realm):
        bases = tuple()
        if self.base:
            factory = realm.getfactory()

            for parent in self.base:
                bases += (factory(parent),)

        bases += (semantix.caos.concept.Concept,)
        return bases


class LinkProperty(GraphObject):
    def __init__(self, name, atom, backend=None, title=None, description=None):
        super().__init__(name, backend=backend, title=title, description=description)
        self.atom = atom
        self.mods = {}


class Link(GraphObject):
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
        name, bases, dct = super().get_class_template(realm)
        dct.update({'source': self.source, 'target': self.target, 'required': self.required})

        if self.mapping:
            dct['mapping'] = self.mapping

        if self.properties:
            dct['properties'] = self.properties

        return (name, bases, dct)

    def get_class_base(self, realm):
        bases = tuple()
        if self.base:
            factory = realm.getfactory()

            for parent in self.base:
                bases += (factory(parent),)

        bases += (semantix.caos.link.Link,)
        return bases

    @classmethod
    def gen_link_name(cls, source, target, basename):
        # XXX: Determine if it makes sense to use human-generatable name here
        hash = hashlib.md5(str(source).encode() + str(target).encode()).hexdigest()
        return '%s_%s' % (basename, hash)

    def atomic(self):
        return (self.target and isinstance(self.target, Atom)) or self.atom


class RealmMetaIterator(object):
    def __init__(self, index, type, include_automatic=False):
        self.index = index
        self.type = type

        sourceset = self.index.index

        if type is not None:
            itertype = index.index_by_type[type]

            if sourceset:
                sourceset = itertype & sourceset
            else:
                sourceset = itertype

        filtered = sourceset - index.index_builtin
        if not include_automatic:
            filtered -= index.index_automatic
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

        if isinstance(obj, Atom):
            if obj.name.module == 'builtin':
                self.index_builtin.add(obj)
            elif obj.automatic:
                self.index_automatic.add(obj)

    def add_module(self, module, alias):
        existing = self.modules.get(alias)
        if existing and existing != module:
            raise MetaError('Alias %s is already bound to module %s' % (alias, self.modules[alias]))
        else:
            self.modules[alias] = module
            self.rmodules.add(module)

    def get(self, name, default=MetaError, module_aliases=None, type=None):
        obj = self.lookup_name(name, module_aliases, default=None)
        if not obj:
            if default and issubclass(default, MetaError):
                raise default('reference to a non-existent semantic graph node: %s' % name)
            else:
                obj = default
        if type and not isinstance(obj, type):
            if default and issubclass(default, MetaError):
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

    def __call__(self, type=None, include_automatic=False):
        return RealmMetaIterator(self, type, include_automatic)

    def __contains__(self, obj):
        return obj in self.index

    def normalize_name(self, name, module_aliases=None, default=MetaError):
        name, module, nqname = self._split_name(name)
        norm_name = None

        if module is None:
            object = None
            default_module = self.resolve_module(module, module_aliases)

            if default_module:
                object = self.lookup_qname(CaosName(name=nqname, module=default_module))
            if not object:
                object = self.lookup_qname(CaosName(name=nqname, module='builtin'))
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
                    norm_name = CaosName(name=nqname, module=fullmodule)

        if norm_name:
            return norm_name
        else:
            if default and issubclass(default, MetaError):
                raise default('could not normalize caos name %s' % name)
            else:
                return default

    def resolve_module(self, module, module_aliases):
        if module_aliases:
            module = module_aliases.get(module, self.modules.get(module))
        else:
            module = self.modules.get(module)
        return module

    def lookup_name(self, name, module_aliases=None, default=MetaError):
        name = self.normalize_name(name, module_aliases, default=default)
        if name:
            return self.lookup_qname(name)

    def lookup_qname(self, name):
        module = self.index_by_module.get(name.module)
        if module:
            return module.get(name.name)

    def _init_builtin(self):
        for clsname in BuiltinAtom.base_atoms_to_class_map:
            atom = BuiltinAtom(name=CaosName(name=clsname, module='builtin'))
            self.add(atom)
        self.add_module('builtin', 'builtin')

    def _split_name(self, name):
        if isinstance(name, CaosName):
            module = name.module
            nqname = name.name
        elif isinstance(name, tuple):
            module = name[0]
            nqname = name[1]
            name = module + '.' + nqname if module else nqname
        elif CaosName.is_qualified(name):
            name = CaosName(name)
            module = name.module
            nqname = name.name
        else:
            module = None
            nqname = name

        return name, module, nqname
