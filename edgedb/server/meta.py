import re
import collections
import semantix.caos.atom
import semantix.caos.concept
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


class GraphObject(object):
    def __init__(self, name, backend=None, base=None):
        self.name = name
        self.base = base if base else tuple()
        self.backend = backend

    def get_class_template(self, realm):
        """
            Return a tuple (bases, classdict) to be used to
            construct a class representing the graph object
        """
        pass

    def merge(self, obj):
        if not isinstance(obj, self.__class__):
            raise MetaMismatchError("cannot merge instances of %s and %s" % (obj.__class__.__name__, self.__class__.__name__))


class AtomMod(object):
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


class Atom(GraphObject):
    def __init__(self, name, backend=None, base=None, default=None, builtin=False, automatic=False):
        super().__init__(name, backend, base)
        self.mods = {}
        self.default = default
        self.builtin = builtin
        self.automatic = automatic

    def add_mod(self, mod):
        if mod.__class__ not in self.mods:
            self.mods[mod.__class__] = mod
        else:
            self.mods[mod.__class__].merge(mod)

    def get_class_template(self, realm):
        bases = tuple()

        if isinstance(self.base, str):
            base = realm.getfactory()(self.base)
        else:
            base = self.base

        dct = {'name': self.name, 'mods': self.mods, 'base': base, 'realm': realm, 'backend': self.backend}

        if self.default is not None:
            dct['default'] = base(self.default)
        else:
            dct['default'] = None

        if self.builtin:
            bases = (semantix.caos.atom.Atom, base)
        else:
            bases = (base, semantix.caos.atom.Atom)

        return (bases, dct)


class Concept(GraphObject):
    def __init__(self, name, backend=None, base=None):
        super().__init__(name, backend, base)

        self.links = {}

    def merge(self, other):
        super().merge(other)

        self.links.update(other.links)

    def add_link(self, link):
        if link.link_type in self.links:
            self.links[link.link_type].targets.update(link.targets)
        else:
            self.links[link.link_type] = link

    def get_class_template(self, realm):
        dct = {'concept': self.name, 'links': self.links, 'parents': self.base, 'realm': realm, 'backend': self.backend}

        bases = tuple()
        if self.base:
            factory = realm.getfactory()

            for parent in self.base:
                bases += (factory(parent),)

        bases += (semantix.caos.concept.Concept,)

        return (bases, dct)


class ConceptLink(object):
    __slots__ = ['source', 'targets', 'link_type', 'mapping', 'required']

    def __init__(self, source, targets, link_type, mapping='11', required=False):
        self.source = source
        self.targets = targets
        self.link_type = link_type
        self.mapping = mapping
        self.required = required

    def atomic(self):
        return len(self.targets) == 1 and isinstance(list(self.targets)[0], Atom)


class RealmMetaIterator(object):
    def __init__(self, index, type):
        self.index = index
        self.type = type

        sourceset = self.index.index

        if type is not None:
            itertype = index.index_by_type[type]

            if sourceset:
                sourceset = itertype & sourceset
            else:
                sourceset = itertype

        filtered = sourceset - index.index_builtin - index.index_automatic
        self.iter = iter(filtered)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iter)


class RealmMeta(object):

    def __init__(self):
        self.index = OrderedSet()
        self.index_by_type = {'concept': OrderedSet(), 'atom': OrderedSet()}
        self.index_by_name = collections.OrderedDict()
        self.index_builtin = OrderedSet()
        self.index_automatic = OrderedSet()
        self.index_by_backend = {}

        self._init_builtin()

    def add(self, obj):
        self.index.add(obj)
        type = 'atom' if isinstance(obj, Atom) else 'concept'
        self.index_by_type[type].add(obj)
        self.index_by_name[obj.name] = obj

        if obj.backend:
            self.index_by_backend[obj.backend] = obj

        if isinstance(obj, Atom):
            if obj.builtin:
                self.index_builtin.add(obj)
            elif obj.automatic:
                self.index_automatic.add(obj)

    def get(self, name):
        return self.index_by_name.get(name)

    def backends(self):
        return list(self.index_by_backend.keys())

    def __iter__(self):
        return RealmMetaIterator(self, None)

    def __call__(self, type=None):
        return RealmMetaIterator(self, type)

    def __contains__(self, obj):
        return obj in self.index

    def _init_builtin(self):
        base_atoms_to_class_map = {
                                    'str': str,
                                    'int': int,
                                    'float': float,
                                    'bool': Bool
                                  }

        for clsname, baseclass in base_atoms_to_class_map.items():
            atom = Atom(name=clsname, base=baseclass, builtin=True)
            self.add(atom)
