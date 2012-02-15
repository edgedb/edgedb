##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import io

import importlib
import collections
import itertools
import decimal
import sys

from semantix.utils import lang
from semantix.utils.lang import context as lang_context
from semantix.utils.lang import yaml
from semantix.utils.lang import protoschema as lang_protoschema
from semantix.utils.lang.yaml import protoschema as yaml_protoschema
from semantix.utils.lang.yaml.struct import StructMeta
from semantix.utils.nlang import morphology
from semantix.utils.algos.persistent_hash import persistent_hash
from semantix.utils.algos import topological
from semantix.utils.datastructures import xvalue, OrderedSet

from semantix import caos
from semantix.caos import proto
from semantix.caos import backends
from semantix.caos import delta as base_delta
from semantix.caos import objects
from semantix.caos.caosql import expr as caosql_expr
from semantix.caos.caosql import errors as caosql_exc

from . import delta


class MetaError(yaml_protoschema.SchemaError, caos.MetaError):
    pass


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


class Bool(yaml.Object, adapts=objects.boolean.Bool, ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return bool(data)


class TimeDelta(yaml.Object, adapts=objects.datetime.TimeDelta, ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class Int(yaml.Object, adapts=objects.int.Int, ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return int(data)


class DecimalMeta(LangObjectMeta, type(objects.numeric.Decimal)):
    pass


class Decimal(yaml.Object, metaclass=DecimalMeta,
              adapts=objects.numeric.Decimal, ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)



class WordCombination(LangObject, adapts=morphology.WordCombination, ignore_aliases=True):
    def __new__(cls, data):
        if isinstance(data, dict):
            word = morphology.WordCombination.from_dict(data)
            data = word.forms.values()

        self = morphology.WordCombination.__new__(cls, data)

        return self

    @classmethod
    def __sx_getnewargs__(cls, context, data):
        return (data,)

    def __reduce__(self):
        return (self.__class__, (tuple(self.forms.values()),))

    @classmethod
    def __sx_getstate__(cls, data):
        return data.as_dict()

    @classmethod
    def adapt(cls, obj):
        return cls.from_dict(obj)


class StrLangObject(LangObject):
    def __reduce__(self):
        return (self.__class__.get_adaptee(), (str(self),))

    def __getstate__(self):
        return {}

    @classmethod
    def __sx_getnewargs__(cls, context, data):
        return (data,)


class LinkMapping(StrLangObject, adapts=caos.types.LinkMapping, ignore_aliases=True):
    def __new__(cls, data):
        return caos.types.LinkMapping.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class PointerLoading(StrLangObject, adapts=caos.types.PointerLoading, ignore_aliases=True):
    def __new__(cls, data):
        return caos.types.PointerLoading.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class LinkSearchWeight(StrLangObject, adapts=caos.types.LinkSearchWeight, ignore_aliases=True):
    def __new__(cls, data):
        return caos.types.LinkSearchWeight.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class PrototypeMeta(LangObjectMeta, StructMeta):
    pass


class Prototype(LangObject, adapts=proto.Prototype, metaclass=PrototypeMeta):
    pass


class DefaultSpec(LangObject, adapts=proto.DefaultSpec, ignore_aliases=True):
    @classmethod
    def resolve(cls, data):
        if isinstance(data, dict) and 'query' in data:
            return QueryDefaultSpec
        else:
            return LiteralDefaultSpec


class LiteralDefaultSpec(DefaultSpec, adapts=proto.LiteralDefaultSpec):
    def __sx_setstate__(self, data):
        proto.LiteralDefaultSpec.__init__(self, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return data.value


class QueryDefaultSpec(DefaultSpec, adapts=proto.QueryDefaultSpec):
    def __sx_setstate__(self, data):
        proto.QueryDefaultSpec.__init__(self, data['query'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'query': str(data.value)}


class AtomConstraint(LangObject, ignore_aliases=True):
    pass


class AtomConstraintMinLength(AtomConstraint, adapts=proto.AtomConstraintMinLength):
    def __sx_setstate__(self, data):
        proto.AtomConstraintMinLength.__init__(self, data['min-length'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'min-length': data.value}


class AtomConstraintMinValue(AtomConstraint, adapts=proto.AtomConstraintMinValue):
    def __sx_setstate__(self, data):
        proto.AtomConstraintMinValue.__init__(self, data['min-value'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'min-value': data.value}


class AtomConstraintMinExValue(AtomConstraint, adapts=proto.AtomConstraintMinExValue):
    def __sx_setstate__(self, data):
        proto.AtomConstraintMinExValue.__init__(self, data['min-value-ex'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'min-value-ex': data.value}


class AtomConstraintMaxLength(AtomConstraint, adapts=proto.AtomConstraintMaxLength):
    def __sx_setstate__(self, data):
        proto.AtomConstraintMaxLength.__init__(self, data['max-length'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'max-length': data.value}


class AtomConstraintMaxValue(AtomConstraint, adapts=proto.AtomConstraintMaxValue):
    def __sx_setstate__(self, data):
        proto.AtomConstraintMaxValue.__init__(self, data['max-value'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'max-value': data.value}


class AtomConstraintMaxExValue(AtomConstraint, adapts=proto.AtomConstraintMaxExValue):
    def __sx_setstate__(self, data):
        proto.AtomConstraintMaxValue.__init__(self, data['max-value-ex'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'max-value-ex': data.value}


class AtomConstraintPrecision(AtomConstraint, adapts=proto.AtomConstraintPrecision):
    def __sx_setstate__(self, data):
        if isinstance(data['precision'], int):
            precision = (int(data['precision']), 0)
        else:
            precision = int(data['precision'][0])
            scale = int(data['precision'][1])

            if scale >= precision:
                raise ValueError('Scale must be strictly less than total numeric precision')

            precision = (precision, scale)
        proto.AtomConstraintPrecision.__init__(self, precision)

    @classmethod
    def __sx_getstate__(cls, data):
        if data.value[1] is None:
            return {'precision': data.value[0]}
        else:
            return {'precision': list(data.value)}


class AtomConstraintRounding(AtomConstraint, adapts=proto.AtomConstraintRounding):
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

    def __sx_setstate__(self, data):
        proto.AtomConstraintRounding.__init__(self, self.map[data['rounding']])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'rounding': cls.rmap[data.value]}


class AtomConstraintExpr(AtomConstraint, adapts=proto.AtomConstraintExpr):
    def __sx_setstate__(self, data):
        proto.AtomConstraintExpr.__init__(self, [data['expr'].strip(' \n')])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'expr': next(iter(data.values))}


class AtomConstraintRegExp(AtomConstraint, adapts=proto.AtomConstraintRegExp):
    def __sx_setstate__(self, data):
        proto.AtomConstraintRegExp.__init__(self, [data['regexp']])

    @classmethod
    def __sx_getstate__(self, data):
        return {'regexp': next(iter(data.values))}

default_name = None

class Atom(Prototype, adapts=proto.Atom):
    def __sx_setstate__(self, data):
        default = data['default']
        if default and not isinstance(default, list):
            default = [default]

        proto.Atom.__init__(self, name=default_name, base=data['extends'],
                            default=default, title=data['title'],
                            description=data['description'], is_abstract=data['abstract'],
                            is_final=data['final'],
                            attributes=data.get('attributes'),
                            _setdefaults_=False, _relaxrequired_=True)
        self._constraints = data.get('constraints')

    @classmethod
    def __sx_getstate__(cls, data):
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

        if data.is_final:
            result['final'] = data.is_final

        if data.constraints:
            result['constraints'] = sorted(list(itertools.chain.from_iterable(data.constraints.values())),
                                           key=lambda i: i.__class__.constraint_name)

        if data.attributes:
            result['attributes'] = dict(data.attributes)

        return result


class Concept(Prototype, adapts=proto.Concept):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.Concept.__init__(self, name=default_name,
                               base=tuple(extends) if extends else tuple(),
                               title=data.get('title'), description=data.get('description'),
                               is_abstract=data.get('abstract'), is_final=data.get('final'),
                               _setdefaults_=False, _relaxrequired_=True)
        self._links = data.get('links', {})
        self._computables = data.get('computables', {})
        self._indexes = data.get('indexes') or ()

    @classmethod
    def __sx_getstate__(cls, data):
        result = {
            'extends': list(itertools.chain(data.base, data.custombases))
        }

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.is_abstract:
            result['abstract'] = data.is_abstract

        if data.is_final:
            result['final'] = data.is_final

        if data.own_pointers:
            result['links'] = {}
            result['computables'] = {}
            for ptr_name, ptr in data.own_pointers.items():
                if isinstance(ptr.target, proto.Atom) and ptr.target.automatic:
                    key = ptr.target.base
                else:
                    if isinstance(ptr.target, proto.Concept) and ptr.target.is_virtual:
                        key = tuple(t.name for t in ptr.target.children())
                    else:
                        key = ptr.target.name

                section = 'computables' if isinstance(ptr, proto.Computable) else 'links'

                result[section][ptr_name] = {key: ptr}

        if data.indexes:
            result['indexes'] = list(sorted(data.indexes, key=lambda i: i.expr))

        return result

    def process_index_expr(self, index):
        return index

    def materialize(self, meta):
        indexes = set()
        for index in self.indexes:
            indexes.add(self.process_index_expr(index))
        self.indexes = indexes

        proto.Concept.materialize(self, meta)


class SourceIndex(LangObject, adapts=proto.SourceIndex, ignore_aliases=True):
    def __sx_setstate__(self, data):
        proto.SourceIndex.__init__(self, expr=data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data.expr)


class LinkPropertyDef(Prototype, proto.LinkProperty):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.LinkProperty.__init__(self, name=default_name, title=data['title'],
                                    base=tuple(extends) if extends else tuple(),
                                    description=data['description'], readonly=data['readonly'],
                                    loading=data['loading'],
                                    _setdefaults_=False, _relaxrequired_=True)

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.generic():
            if data.base:
                result['extends'] = list(data.base)

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.readonly:
            result['readonly'] = data.readonly

        if data.loading:
            result['loading'] = data.loading

        return result


class LinkProperty(Prototype, adapts=proto.LinkProperty, ignore_aliases=True):
    def __sx_setstate__(self, data):
        if isinstance(data, str):
            proto.LinkProperty.__init__(self, name=default_name, target=data, _relaxrequired_=True)
        else:
            atom_name, info = next(iter(data.items()))

            default = info['default']
            if default and not isinstance(default, list):
                default = [default]

            proto.LinkProperty.__init__(self, name=default_name, target=atom_name,
                                        title=info['title'], description=info['description'],
                                        readonly=info['readonly'], default=default,
                                        loading=info['loading'], required=info['required'],
                                        _setdefaults_=False, _relaxrequired_=True)
            self._constraints = info.get('constraints')
            self._abstract_constraints = info.get('abstract-constraints')

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.target and data.target.constraints and data.target.automatic:
            items = itertools.chain.from_iterable(data.target.constraints.values())
            result['constraints'] = list(items)

        if data.local_constraints:
            constraints = result.setdefault('constraints', [])
            constraints.extend(itertools.chain.from_iterable(data.local_constraints.values()))

        if data.abstract_constraints:
            items = itertools.chain.from_iterable(data.local_constraints.values())
            result['abstract-constraints'] = list(items)

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.default is not None:
            result['default'] = data.default

        if result:
            if data.target:
                return {data.target.name: result}
            else:
                return result
        else:
            if data.target:
                return str(data.target.name)
            else:
                return {}


class LinkPropertyProps(Prototype, proto.LinkProperty):
    def __sx_setstate__(self, data):
        default = data['default']
        if default and not isinstance(default, list):
            default = [default]

        proto.LinkProperty.__init__(self, name=default_name, default=default,
                                    _setdefaults_=False, _relaxrequired_=True)


class LinkDef(Prototype, adapts=proto.Link):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        default = data['default']
        if default and not isinstance(default, list):
            default = [default]

        proto.Link.__init__(self, name=default_name,
                            base=tuple(extends) if extends else tuple(),
                            title=data['title'], description=data['description'],
                            is_abstract=data.get('abstract'), is_final=data.get('final'),
                            readonly=data.get('readonly'),
                            mapping=data.get('mapping'),
                            loading=data.get('loading'),
                            default=default,
                            _setdefaults_=False, _relaxrequired_=True)

        self._properties = data['properties']
        self._computables = data.get('computables', {})
        self._indexes = data.get('indexes') or ()

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.generic():
            if data.base:
                result['extends'] = list(data.base)

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.is_abstract:
            result['abstract'] = data.is_abstract

        if data.is_final:
            result['final'] = data.is_final

        if data.readonly:
            result['readonly'] = data.readonly

        if data.loading:
            result['loading'] = data.loading

        if data.mapping:
            result['mapping'] = data.mapping

        if isinstance(data.target, proto.Atom) and data.target.automatic:
            result['constraints'] = list(itertools.chain.from_iterable(data.target.constraints.values()))

        if data.required:
            result['required'] = data.required

        if data.default is not None:
            result['default'] = data.default

        if data.own_pointers:
            result['properties'] = {}
            result['computables'] = {}
            for ptr_name, ptr in data.own_pointers.items():
                if isinstance(ptr, proto.Computable):
                    result['computables'][ptr_name] = ptr
                else:
                    result['properties'][ptr_name] = ptr

        if data.local_constraints:
            constraints = result.setdefault('constraints', [])
            constraints.extend(itertools.chain.from_iterable(data.local_constraints.values()))

        if data.abstract_constraints:
            items = itertools.chain.from_iterable(data.abstract_constraints.values())
            result['abstract-constraints'] = list(items)

        if data.search:
            result['search'] = data.search

        if data.indexes:
            result['indexes'] = list(sorted(data.indexes, key=lambda i: i.expr))

        return result


class Computable(Prototype, adapts=proto.Computable):
    def __sx_setstate__(self, data):
        if isinstance(data, str):
            data = {'expression': data}

        proto.Computable.__init__(self, expression=data.get('expression'),
                                  name=default_name, source=None,
                                  title=data.get('title'),
                                  description=data.get('description'),
                                  _setdefaults_=False,
                                  _relaxrequired_=True)

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        result['expression'] = data.expression
        return result


class PointerConstraint(LangObject, adapts=proto.PointerConstraint, ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return {cls.constraint_name: next(iter(data.values))}


class PointerConstraintUnique(PointerConstraint, adapts=proto.PointerConstraintUnique):
    def __sx_setstate__(self, data):
        values = {data[self.__class__.constraint_name]}
        proto.PointerConstraintUnique.__init__(self, values)


class LinkSearchConfiguration(LangObject, adapts=proto.LinkSearchConfiguration, ignore_aliases=True):
    def __sx_setstate__(self, data):
        if isinstance(data, bool):
            if data:
                weight = caos.types.SearchWeight_A
            else:
                weight = None
        else:
            if data:
                weight = caos.types.LinkSearchWeight(data['weight'])
            else:
                weight = None

        proto.LinkSearchConfiguration.__init__(self, weight=weight)

    @classmethod
    def __sx_getstate__(cls, data):
        if data.weight:
            return {'weight': data.weight}
        else:
            return None


class SpecializedLink(LangObject):

    def __sx_setstate__(self, data):
        context = lang_context.SourceContext.from_object(self)

        if isinstance(data, (str, list)):
            link = proto.Link(source=None, target=None, name=default_name, _setdefaults_=False,
                              _relaxrequired_=True)
            lang_context.SourceContext.register_object(link, context)
            link._targets = (data,) if isinstance(data, str) else data
            self.link = link

        elif isinstance(data, dict):
            if len(data) != 1:
                raise MetaError('unexpected number of elements in link data dict: %d', len(data),
                                context=context)

            targets, info = next(iter(data.items()))

            if not isinstance(targets, tuple):
                targets = (targets,)

            default = info['default']
            if default and not isinstance(default, list):
                default = [default]

            props = info['properties']

            link = proto.Link(name=default_name, target=None, mapping=info['mapping'],
                              required=info['required'], title=info['title'],
                              description=info['description'], readonly=info['readonly'],
                              loading=info['loading'],
                              default=default,
                              _setdefaults_=False, _relaxrequired_=True)

            search = info.get('search')
            if search and search.weight is not None:
                link.search = search

            lang_context.SourceContext.register_object(link, context)

            link._constraints = info.get('constraints')
            link._abstract_constraints = info.get('abstract-constraints')
            link._properties = props
            link._targets = targets

            self.link = link
        else:
            raise MetaError('unexpected specialized link format: %s', type(data), context=context)


class ProtoSchemaAdapter(yaml_protoschema.ProtoSchemaAdapter):
    def load_imports(self, context, localschema):
        this_module = self.module.name

        imports = context.document.imports.copy()

        self.module.imports = tuple(m.__name__ for m in imports.values())

        if this_module != localschema.builtins_module:
            # Add implicit builtins import
            builtins = importlib.import_module(localschema.builtins_module)
            imports[localschema.builtins_module] = builtins

        for alias, module in imports.items():
            # module may not be a module objects, but just a shim
            module = sys.modules[module.__name__]

            try:
                proto_module = module.__sx_prototypes__
            except AttributeError:
                localschema.add_module(module, alias=alias)
            else:
                localschema.add_module(proto_module, alias=alias)

            lang_protoschema.populate_proto_modules(localschema, module)


    def _add_proto(self, schema, proto):
        schema.add(proto)
        self._add_foreign_proto(proto)


    def _add_foreign_proto(self, proto):
        context = lang_context.SourceContext.from_object(self)
        this_module = context.document.import_context

        if proto.name.module != this_module:
            proto_module = importlib.import_module(proto.name.module)

            try:
                proto_module.__sx_prototypes__.get(proto.name.name, type=proto.get_canonical_class())
            except caos.MetaError:
                proto_module.__sx_prototypes__.add(proto)


    def read_elements(self, data, localschema):
        self.caosql_expr = caosql_expr.CaosQLExpression(localschema, localschema.modules)
        self.read_atoms(data, localschema)
        self.read_link_properties(data, localschema)
        self.read_links(data, localschema)
        self.read_concepts(data, localschema)

    def order_elements(self, localschema):
        # The final pass on may produce additional objects,
        # thus, it has to be performed in reverse order.
        concepts = OrderedSet(self.order_concepts(localschema))
        links = OrderedSet(self.order_links(localschema))
        linkprops = OrderedSet(self.order_link_properties(localschema))
        computables = OrderedSet(self.order_computables(localschema))
        atoms = OrderedSet(self.order_atoms(localschema))

        for atom in atoms:
            atom.setdefaults()

        for comp in computables:
            comp.setdefaults()

        for prop in linkprops:
            prop.setdefaults()

        for link in links:
            link.setdefaults()

        for link in links:
            link.materialize(localschema)

        for concept in concepts:
            concept.setdefaults()

        for concept in concepts:
            concept.materialize(localschema)

        # Link meterialization might have produced new specialized properties
        for atom in localschema(type=caos.proto.Atom, include_automatic=True):
            if atom.name.module == self.module.name:
                atoms.add(atom)

        # Link meterialization might have produced new specialized properties
        for prop in localschema(type=caos.proto.LinkProperty, include_automatic=True):
            if prop.name.module != self.module.name:
                self._add_foreign_proto(prop)
            else:
                linkprops.add(prop)

        # Concept meterialization might have produced new specialized link
        for link in localschema(type=caos.proto.Link, include_automatic=True):
            if link.name.module != self.module.name:
                self._add_foreign_proto(link)
            else:
                links.add(link)

        for link in localschema(type=caos.proto.Computable, include_automatic=True):
            if link.name.module != self.module.name:
                self._add_foreign_proto(link)
            else:
                links.add(link)

        # Arrange prototypes in the resulting schema according to determined topological order.
        localschema.reorder(itertools.chain(atoms, computables, linkprops, links, concepts))


    def get_proto_schema_class(self):
        return proto.ProtoSchema


    def get_proto_module_class(self):
        return proto.ProtoModule


    def get_schema_name_class(self):
        return caos.Name


    def _check_base(self, element, base_name, localschema):
        base = localschema.get(base_name, type=element.__class__.get_canonical_class(),
                               include_pyobjects=True, index_only=False)
        if isinstance(base, caos.types.ProtoObject) and base.is_final:
            context = lang_context.SourceContext.from_object(element)
            raise MetaError('"%s" is final and cannot be inherited from' % base.name,
                            context=context)
        return base


    def read_atoms(self, data, localschema):
        for atom_name, atom in data['atoms'].items():
            atom.name = caos.Name(name=atom_name, module=self.module.name)
            self._add_proto(localschema, atom)

        for atom in self.module('atom'):
            if atom.base:
                try:
                    base = self._check_base(atom, atom.base, localschema)

                    if isinstance(base, caos.types.ProtoAtom):
                        atom.base = base.name
                    else:
                        atom.base = '{}.{}'.format(base.__module__, base.__name__)
                except caos.MetaError as e:
                    context = lang_context.SourceContext.from_object(atom)
                    raise MetaError(e, context=context) from e


    def order_atoms(self, localschema):
        context = lang_context.SourceContext.from_object(self)
        this_module = context.document.import_context

        g = {}

        for atom in self.module('atom', include_automatic=True):
            g[atom.name] = {"item": atom, "merge": [], "deps": []}

            if atom.name.module == this_module:
                constraints = getattr(atom, '_constraints', None)
                if constraints:
                    atom.normalize_constraints(localschema, constraints)
                    for constraint in constraints:
                        atom.add_constraint(constraint)

                if atom.base:
                    atom_base = localschema.get(atom.base, include_pyobjects=True, index_only=False)
                    if isinstance(atom_base, proto.Atom) and atom.name:
                        atom.base = atom_base.name
                        g[atom.name]['merge'].append(atom.base)
                        if atom_base.name.module != self.module.name:
                            g[atom_base.name] = {"item": atom_base, "merge": [], "deps": []}

        atoms = topological.normalize(g, merger=proto.Atom.merge)
        return list(filter(lambda a: a.name.module == self.module.name, atoms))

    def add_pointer_constraints(self, parent, constraints, type, constraint_type='regular'):
        if constraints:
            for constraint in constraints:
                if isinstance(constraint, proto.PointerConstraint):
                    if isinstance(constraint, proto.PointerConstraintUnique):
                        if type == 'atom':
                            if len(constraint.values) > 1 \
                                    or isinstance(list(constraint.values)[0], str):
                                raise caos.MetaError(('invalid value for atomic pointer "%s" '
                                                      'unique constraint') % parent.normal_name())
                        elif type == 'concept':
                            if not isinstance(list(constraint.values)[0], str):
                                raise caos.MetaError(('invalid value for non-atomic pointer "%s" '
                                                      'unique constraint, expecting an expression')\
                                                      % parent.normal_name())

                    if constraint_type == 'abstract':
                        parent.add_abstract_constraint(constraint)
                    else:
                        parent.add_constraint(constraint)

    def read_link_properties(self, data, localschema):
        for property_name, property in data['link-properties'].items():
            module = self.module.name
            property.name = caos.Name(name=property_name, module=module)

            self._add_proto(localschema, property)

        for prop in self.module('link_property'):
            if prop.base:
                bases = []
                for base_name in prop.base:
                    base = self._check_base(prop, base_name, localschema)
                    bases.append(base.name)
                prop.base = tuple(bases)
            elif prop.name != 'semantix.caos.builtins.link_property':
                prop.base = (caos.Name('semantix.caos.builtins.link_property'),)


    def order_link_properties(self, localschema):
        g = {}

        for prop in self.module('link_property', include_automatic=True):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}

            if prop.base:
                g[prop.name]['merge'].extend(prop.base)

                for b in prop.base:
                    base = self._check_base(prop, b, localschema)

                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}

        p = topological.normalize(g, merger=proto.LinkProperty.merge)
        return list(filter(lambda p: p.name.module == self.module.name, p))


    def read_properties_for_link(self, link, localschema):
        props = getattr(link, '_properties', None)
        if not props:
            return

        for property_name, property in props.items():

            property_base = localschema.get(property_name, type=proto.LinkProperty, default=None,
                                            index_only=False)

            if property_base is None:
                if not link.generic():
                    # Only generic links can implicitly define properties
                    raise caos.MetaError('reference to an undefined property "%s"' % property_name)

                # The link property has not been defined globally.
                if not caos.Name.is_qualified(property_name):
                    # If the name is not fully qualified, assume inline link property
                    # definition. The only attribute that is used for global definition
                    # is the name.
                    property_qname = caos.Name(name=property_name, module=self.module.name)
                    propdef = proto.LinkProperty(name=property_qname,
                                    base=(caos.Name('semantix.caos.builtins.link_property'),))
                    self._add_proto(localschema, propdef)
                else:
                    property_qname = caos.Name(property_name)
            else:
                property_qname = property_base.name

            if link.generic():
                target = localschema.get(property.target)
                property.target = target.name
            else:
                link_base = localschema.get(link.base[0], type=proto.Link, index_only=False)
                propdef = link_base.pointers.get(property_qname)
                if not propdef:
                    raise caos.MetaError('link "%s" does not define property "%s"' \
                                         % (link.name, property_qname))
                property_qname = propdef.normal_name()

            # A new specialized subclass of the link property is created for each
            # (source, property_name, target_atom) combination
            property.base = (property_qname,)
            prop_genname = proto.LinkProperty.generate_specialized_name(link.name, property.target,
                                                                        property_qname)
            property.name = caos.Name(name=prop_genname, module=self.module.name)
            property.source = link

            self.add_pointer_constraints(property, getattr(property, '_constraints', ()), 'atom')
            self.add_pointer_constraints(property, getattr(property, '_abstract_constraints', ()),
                                                                     'atom', 'abstract')

            self._add_proto(localschema, property)

            link.add_pointer(property)

    def _create_base_link(self, link, link_qname, localschema, type=None):
        type = type or proto.Link

        base = 'semantix.caos.builtins.link' if type is proto.Link else \
               'semantix.caos.builtins.link_property'

        linkdef = type(name=link_qname, base=(caos.Name(base),), _setdefaults_=False)

        self._add_proto(localschema, linkdef)
        return linkdef

    def _read_computables(self, source, localschema):
        for cname, computable in getattr(source, '_computables', {}).items():
            computable_base = localschema.get(cname, type=proto.Computable, default=None,
                                              index_only=False)

            if computable_base is None:
                if not caos.Name.is_qualified(cname):
                    computable_qname = caos.Name(name=cname, module=self.module.name)
                else:
                    computable_qname = caos.Name(cname)
            else:
                computable_qname = computable_base.name

            if computable_qname in source.own_pointers:
                context = lang_context.SourceContext.from_object(computable)
                raise MetaError('computable "%(name)s" conflicts with "%(name)s" pointer '
                                'defined in the same source' % {'name': computable_qname},
                                 context=context)

            computable_name = proto.Computable.generate_specialized_name(source.name, None,
                                                                         computable_qname)
            computable.source = source
            computable.name = caos.Name(name=computable_name, module=self.module.name)
            computable.setdefaults()
            source.add_pointer(computable)

            super = localschema.get(computable.normal_name(), default=None, index_only=False)
            if super is None:
                type = proto.Link if isinstance(source, proto.Concept) else proto.LinkProperty
                super = self._create_base_link(computable, computable.normal_name(), localschema,
                                               type=type)

            computable.base = (super.name,)

            self._add_proto(localschema, computable)


    def order_computables(self, localschema):
        return self.module('computable', include_automatic=True)


    def read_links(self, data, localschema):
        for link_name, link in data['links'].items():
            module = self.module.name
            link.name = caos.Name(name=link_name, module=module)

            self.read_properties_for_link(link, localschema)

            self._add_proto(localschema, link)

        for link in self.module('link'):
            if link.base:
                bases = []
                for base_name in link.base:
                    base = self._check_base(link, base_name, localschema)
                    bases.append(base.name)
                link.base = tuple(bases)
            elif link.name != 'semantix.caos.builtins.link':
                link.base = (caos.Name('semantix.caos.builtins.link'),)

            self._read_computables(link, localschema)

            for index in link._indexes:
                expr, tree = self.normalize_index_expr(index.expr, link, localschema)
                idx = proto.SourceIndex(expr, tree=tree)
                context = lang_context.SourceContext.from_object(index)
                lang_context.SourceContext.register_object(idx, context)
                link.add_index(idx)

    def order_links(self, localschema):
        g = {}

        for link in self.module('link', include_automatic=True):
            g[link.name] = {"item": link, "merge": [], "deps": []}

            if link.name.module != self.module.name and \
                                (link.source is None or link.source.name.module != self.module.name):
                continue

            for property_name, property in link.pointers.items():
                if property.target:
                    if not isinstance(property.target, caos.types.ProtoAtom):
                        property.target = localschema.get(property.target, index_only=False)

                    constraints = getattr(property, 'constraints', None)
                    if constraints:
                        atom_constraints = [c for c in constraints.values()
                                            if isinstance(c, proto.AtomConstraint)]
                    else:
                        atom_constraints = None
                    if atom_constraints:
                        # Got an inline atom definition.
                        atom = self.genatom(localschema, link, property.target.name, property_name,
                                            constraints=atom_constraints, default=property.default)
                        self._add_proto(localschema, atom)
                        property.target = atom

            if link.source and not isinstance(link.source, proto.Prototype):
                link.source = localschema.get(link.source)

            if link.target and not isinstance(link.target, proto.Prototype):
                link.target = localschema.get(link.target)

            if link.target:
                link.is_atom = isinstance(link.target, proto.Atom)

            if not link.generic():
                type = 'atom' if link.atomic() else 'concept'

                constraints = getattr(link, '_constraints', ())
                if constraints:
                    link_constraints = [c for c in constraints if isinstance(c, proto.PointerConstraint)]
                    self.add_pointer_constraints(link, link_constraints, type)

                aconstraints = getattr(link, '_abstract_constraints', ())
                if aconstraints:
                    self.add_pointer_constraints(link, aconstraints, type, 'abstract')

            if link.base:
                for base_name in link.base:
                    base = self._check_base(link, base_name, localschema)
                    if base_name.module != self.module.name:
                        g[base_name] = {"item": base, "merge": [], "deps": []}

                g[link.name]['merge'].extend(link.base)

        try:
            links = topological.normalize(g, merger=proto.Link.merge)
        except caos.MetaError as e:
            if e.context:
                raise MetaError(e.msg, hint=e.hint, details=e.details, context=e.context.context) from e
            raise

        links = list(filter(lambda l: l.name.module == self.module.name, links))

        csql_expr = caosql_expr.CaosQLExpression(localschema)

        try:
            for link in links:
                for index in link.own_indexes:
                    csql_expr.check_source_atomic_expr(index.tree, link)

                self.normalize_computables(link, localschema)
                self.normalize_pointer_defaults(link, localschema)

        except caosql_exc.CaosQLReferenceError as e:
            context = lang_context.SourceContext.from_object(index)
            raise MetaError(e.args[0], context=context) from e

        return links

    def read_concepts(self, data, localschema):
        for concept_name, concept in data['concepts'].items():
            concept.name = caos.Name(name=concept_name, module=self.module.name)

            self._add_proto(localschema, concept)

        for concept in self.module('concept'):
            bases = []
            custombases = []

            if concept.base:
                for b in concept.base:
                    base = localschema.get(b, include_pyobjects=True, index_only=False)
                    if isinstance(base, caos.types.ProtoObject):
                        base_name = base.name
                        bases.append(base_name)
                    else:
                        base_name = '{}.{}'.format(base.__module__, base.__name__)

                        if not issubclass(base, caos.concept.Concept):
                            raise caos.MetaError('custom concept base classes must inherit from '
                                                 'caos.concept.Concept: %s' % base_name)
                        custombases.append(base_name)

            if not bases and concept.name != 'semantix.caos.builtins.BaseObject':
                bases.append(caos.Name('semantix.caos.builtins.Object'))

            concept.base = tuple(bases)
            concept.custombases = tuple(custombases)

            for link_name, link in concept._links.items():
                link = link.link
                link_base = localschema.get(link_name, type=proto.Link, default=None,
                                            index_only=False)
                if link_base is None:
                    # The link has not been defined globally.
                    if not caos.Name.is_qualified(link_name):
                        # If the name is not fully qualified, assume inline link definition.
                        # The only attribute that is used for global definition is the name.
                        link_qname = caos.Name(name=link_name, module=self.module.name)
                        self._create_base_link(link, link_qname, localschema)
                    else:
                        link_qname = caos.Name(link_name)
                else:
                    link_qname = link_base.name

                link.source = concept.name

                targets = []
                for t in link._targets:
                    target = localschema.get(t)
                    targets.append(target.name)

                link._targets = targets
                link.target = self._normalize_link_target_name(link_qname, link._targets,
                                                               localschema)

                atom_constraints = self._get_link_atom_constraints(link)

                if atom_constraints:
                    target_name = Atom.gen_atom_name(concept, link_qname)
                    target_name = caos.Name(name=target_name, module=concept.name.module)
                else:
                    target_name = link.target

                # A new specialized subclass of the link is created for each
                # (source, link_name, target) combination
                link.base = (link_qname,)

                link_genname = proto.Link.generate_specialized_name(link.source, target_name,
                                                                    link_qname)
                link.name = caos.Name(name=link_genname, module=self.module.name)

                self.read_properties_for_link(link, localschema)

                self._add_proto(localschema, link)
                concept.add_pointer(link)

        for concept in self.module('concept'):
            for link_name, link in concept._links.items():
                link = link.link
                if len(link._targets) > 1:
                    self._create_link_target(concept, link, localschema)

        for concept in self.module('concept'):
            self._read_computables(concept, localschema)

            for index in getattr(concept, '_indexes', ()):
                expr, tree = self.normalize_index_expr(index.expr, concept, localschema)
                index.expr = expr
                index.tree = tree
                concept.add_index(index)


    def _create_link_target(self, source, pointer, localschema):
        targets = [localschema.get(t, type=proto.Concept) for t in pointer._targets]

        target = pointer.get_common_target(localschema, targets)

        existing = localschema.get(pointer.target, default=None, type=proto.Concept,
                                   index_only=False)
        if not existing:
            self._add_proto(localschema, target)


    def _normalize_link_target_name(self, link_fqname, targets, localschema):
        if len(targets) == 1:
            return targets[0]
        else:
            return proto.Source.gen_virt_parent_name(targets, module=link_fqname.module)


    def normalize_index_expr(self, expr, concept, localschema):
        expr, tree = self.caosql_expr.normalize_source_expr(expr, concept)
        return expr, tree


    def normalize_pointer_defaults(self, source, localschema):
        for link in source.own_pointers.values():
            if isinstance(link, proto.Computable):
                continue

            if link.default:
                for default in link.default:
                    if isinstance(default, QueryDefaultSpec):
                        def_context = lang_context.SourceContext.from_object(default)

                        module_aliases = {None: str(def_context.document.import_context)}
                        for alias, module in def_context.document.imports.items():
                            module_aliases[alias] = module.__name__

                        value, tree = self.caosql_expr.normalize_expr(default.value,
                                                                      module_aliases)

                        first = list(tree.result_types.values())[0][0]
                        if len(tree.result_types) > 1 or not \
                                            first.issubclass(localschema, link.target):
                            raise MetaError(('default value query must yield a '
                                             'single-column result of type "%s"') %
                                             link.target.name, context=def_context)

                        if not isinstance(link.target, caos.types.ProtoAtom):
                            if link.mapping not in (caos.types.ManyToOne,
                                                    caos.types.ManyToMany):
                                raise MetaError('concept links with query defaults ' \
                                                'must have either a "*1" or "**" mapping',
                                                 context=def_context)

                        default.value = value
                link.normalize_defaults()


    def normalize_computables(self, source, localschema):
        for link in source.own_pointers.values():
            if not isinstance(link, proto.Computable):
                continue

            src_context = lang_context.SourceContext.from_object(source)
            module_aliases = {None: str(src_context.document.import_context)}
            for alias, module in src_context.document.imports.items():
                module_aliases[alias] = module.__name__

            expression, tree = self.caosql_expr.normalize_expr(link.expression,
                                                               module_aliases,
                                                               anchors={'self': source})
            refs = self.caosql_expr.get_node_references(tree)

            expression = self.caosql_expr.normalize_refs(link.expression, module_aliases)

            first = list(tree.result_types.values())[0][0]

            assert first, "Could not determine computable expression result type"

            if len(tree.result_types) > 1:
                link_context = lang_context.SourceContext.from_object(link)
                raise MetaError(('computable expression must yield a '
                                 'single-column result'), context=link_context)

            if isinstance(source, proto.Link) and not isinstance(first, proto.Atom):
                link_context = lang_context.SourceContext.from_object(link)
                raise MetaError(('computable expression for link property must yield a '
                                 'scalar'), context=link_context)

            link.target = first
            link.expression = expression
            link.is_local = len(refs) == 1 and tuple(refs)[0] is source
            link.is_atom = isinstance(link.target, caos.types.ProtoAtom)

            type = proto.Link if isinstance(source, proto.Concept) else proto.LinkProperty


    def order_concepts(self, localschema):
        g = {}

        for concept in self.module('concept'):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}

            for link_name, link in concept.pointers.items():
                if not isinstance(link.source, proto.Prototype):
                    link.source = localschema.get(link.source)

                if not isinstance(link, proto.Computable) and link.source.name == concept.name:
                    if not isinstance(link.target, proto.Prototype):
                        link.target = localschema.get(link.target, index_only=False)
                        if isinstance(link.target, caos.types.ProtoConcept):
                            link.target.add_rlink(link)

                    if isinstance(link.target, proto.Atom):
                        link.is_atom = True

                        atom_constraints = self._get_link_atom_constraints(link)

                        if atom_constraints and not link.target.name.name.startswith('__'):
                            # Got an inline atom definition.
                            atom = self.genatom(localschema, concept, link.target.name, link_name,
                                                constraints=atom_constraints,
                                                default=link.default)
                            try:
                                localschema.get(atom.name, type=proto.Atom)
                            except caos.MetaError:
                                self._add_proto(localschema, atom)
                            link.target = atom

                        if link.mapping and link.mapping != caos.types.OneToOne:
                            raise caos.MetaError('%s: links to atoms can only have a "1 to 1" mapping'
                                                 % link_name)

            if concept.base:
                for base_name in concept.base:
                    base = self._check_base(concept, base_name, localschema)
                    if base_name.module != self.module.name:
                        g[base_name] = {"item": base, "merge": [], "deps": []}
                g[concept.name]["merge"].extend(concept.base)

        concepts = list(filter(lambda c: c.name.module == self.module.name,
                               topological.normalize(g, merger=proto.Concept.merge)))

        csql_expr = caosql_expr.CaosQLExpression(localschema)

        try:
            for concept in concepts:
                for index in concept.own_indexes:
                    csql_expr.check_source_atomic_expr(index.tree, concept)

                self.normalize_pointer_defaults(concept, localschema)
                self.normalize_computables(concept, localschema)

        except caosql_exc.CaosQLReferenceError as e:
            index_context = lang_context.SourceContext.from_object(index)
            raise MetaError(e.args[0], context=index_context) from e

        return concepts


    def _get_link_atom_constraints(self, link):
        constraints = getattr(link, '_constraints', None)
        if constraints:
            atom_constraints = [c for c in constraints if isinstance(c, proto.AtomConstraint)]
        else:
            atom_constraints = None

        return atom_constraints


    def genatom(self, meta, host, base, link_name, constraints, default):
        atom_name = Atom.gen_atom_name(host, link_name)
        atom = proto.Atom(name=caos.Name(name=atom_name, module=host.name.module),
                          base=base, automatic=True, default=default)
        atom.normalize_constraints(meta, constraints)
        for constraint in constraints:
            atom.add_constraint(constraint)
        return atom


class EntityShellMeta(type(LangObject), type(caos.concept.EntityShell)):
    def __init__(cls, name, bases, dct, *, adapts=None, ignore_aliases=False):
        type(LangObject).__init__(cls, name, bases, dct, adapts=adapts,
                                                         ignore_aliases=ignore_aliases)
        type(caos.concept.EntityShell).__init__(cls, name, bases, dct)


class EntityShell(LangObject, adapts=caos.concept.EntityShell, metaclass=EntityShellMeta):
    def __sx_setstate__(self, data):
        caos.concept.EntityShell.__init__(self)

        if isinstance(data, str):
            self.id = data
        elif isinstance(data, dict) and 'query' in data:
            query = data['query']

            ent_context = lang_context.SourceContext.from_object(self)

            aliases = {alias: mod.__name__ for alias, mod in ent_context.document.imports.items()}
            session = ent_context.document.import_context.session

            selector = session.selector(query, module_aliases=aliases)

            if not isinstance(selector, caos.types.ConceptClass):
                raise MetaError('query expressions must return a single entity')

            selector_iter = iter(selector)

            try:
                self.entity = next(selector_iter)
            except StopIteration:
                raise MetaError('query expressions must return a single entity')

            try:
                next(selector_iter)
            except StopIteration:
                pass
            else:
                raise MetaError('query expressions must return a single entity')

            if data['properties']:
                self.props = data['properties']
        else:
            ent_context = lang_context.SourceContext.from_object(self)

            aliases = {alias: mod.__name__ for alias, mod in ent_context.document.imports.items()}
            session = ent_context.document.import_context.session

            concept, data = next(iter(data.items()))

            links = {}
            props = {}
            for link_name, linkval in data.items():
                if isinstance(linkval, list):
                    links[link_name] = list()
                    for item in linkval:
                        if isinstance(item, dict):
                            links[link_name].append(item['target'])
                            props[(link_name, item['target'])] = item['properties']
                        elif isinstance(item, xvalue):
                            links[link_name].append(item.value)
                            props[(link_name, item.value)] = item.attrs
                        elif isinstance(item, EntityShell):
                            links[link_name].append(item)
                            props[(link_name, item)] = getattr(item, 'props', {})
                        else:
                            links[link_name].append(item)
                else:
                    links[link_name] = linkval

            self.entity = session.schema.get(concept, aliases=aliases)(**links)
            for (link_name, target), link_properties in props.items():
                linkcls = caos.concept.getlink(self.entity, link_name, target)
                linkcls.update(**link_properties)

            ent_context.document.import_context.entities.append(self.entity)


class ProtoSchema(LangObject, adapts=proto.ProtoSchema):
    @classmethod
    def __sx_getstate__(cls, data):
        result = {'atoms': {}, 'links': {}, 'concepts': {}, 'link-properties': {}}

        for type in ('atom', 'link', 'concept', 'link_property'):
            for obj in data(type=type, include_automatic=False):
                # XXX
                if type in ('link', 'link_property') and not obj.generic():
                    continue
                if type == 'link_property':
                    key = 'link-properties'
                else:
                    key = type + 's'

                result[key][str(obj.name)] = obj

        return result


class DataSet(LangObject):
    def __sx_setstate__(self, data):

        entities = {id: [shell.entity for shell in shells] for id, shells in data.items()}
        context = lang_context.SourceContext.from_object(self)
        session = context.document.import_context.session
        with session.transaction():
            for entity in context.document.import_context.entities:
                entity.__class__.materialize_links(entity, entities)


class CaosName(StrLangObject, adapts=caos.Name, ignore_aliases=True):
    def __new__(cls, data):
        return caos.Name.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class ModuleFromData:
    def __init__(self, name):
        self.__name__ = name


class FixtureImportContext(lang.ImportContext):
    def __new__(cls, name, *, loader=None, session=None, entities=None):
        result = super().__new__(cls, name, loader=loader)
        result.session = session
        result.entities = entities if entities is not None else []
        return result

    def __init__(self, name, *, loader=None, session=None, entities=None):
        super().__init__(name, loader=loader)

    @classmethod
    def from_parent(cls, name, parent):
        if parent and isinstance(parent, FixtureImportContext):
            result = cls(name, loader=parent.loader, session=parent.session)
        else:
            result = cls(name)
        return result

    @classmethod
    def copy(cls, name, other):
        if isinstance(other, FixtureImportContext):
            result = cls(other, loader=other.loader, session=other.session)
        else:
            result = cls(other)
        return result


class Backend(backends.MetaBackend):

    def __init__(self, deltarepo, module=None, data=None):
        if module:
            self.metadata = module
            module_name = module.__name__
        else:
            self.metadata = self.load_from_string(data)
            module_name = '<string>'

        modhash = persistent_hash(module_name)

        self._schema = lang_protoschema.get_loaded_proto_schema(self.metadata.__class__)
        self._schema.set_module_alias(self._schema.get_builtins_module(), None)

        repo = deltarepo(module=module_name, id=modhash)
        super().__init__(repo)

    def load_from_string(self, data):
        import_context = proto.ImportContext('<string>')
        module = ModuleFromData('<string>')
        context = lang_context.DocumentContext(module=module, import_context=import_context)
        for k, v in lang.yaml.Language.load_dict(io.StringIO(data), context):
            setattr(module, str(k), v)

        return module

    def getmeta(self):
        return self._schema

    def dump_meta(self, meta):
        prologue = '%SCHEMA semantix.caos.backends.yaml.schemas.Semantics\n---\n'
        return prologue + yaml.Language.dump(meta)
