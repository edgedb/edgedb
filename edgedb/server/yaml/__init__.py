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
from semantix.utils.lang import context as lang_context
from semantix.utils.lang import yaml
from semantix.utils.lang.yaml import protoschema as yaml_protoschema
from semantix.utils.lang.yaml.struct import StructMeta
from semantix.utils.nlang import morphology
from semantix.utils.algos.persistent_hash import persistent_hash
from semantix.utils.algos import topological

from semantix import caos
from semantix.caos import proto
from semantix.caos import backends
from semantix.caos import delta as base_delta
from semantix.caos import objects
from semantix.caos import query as caos_query
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

        proto.Atom.__init__(self, name=default_name, backend=None, base=data['extends'],
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

        proto.Concept.__init__(self, name=default_name, backend=None,
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

        proto.Link.__init__(self, name=default_name, backend=None,
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


class MetaSet(yaml_protoschema.ProtoSchemaAdapter):
    def read_elements(self, data, globalschema, localschema):
        self.caosql_expr = caosql_expr.CaosQLExpression(globalschema, localschema.modules)
        self.read_atoms(data, globalschema, localschema)
        self.read_link_properties(data, globalschema, localschema)
        self.read_links(data, globalschema, localschema)
        self.read_concepts(data, globalschema, localschema)


    def order_elements(self, globalschema):
        # The final pass on may produce additional objects,
        # thus, it has to be performed in reverse order.
        concepts = self.order_concepts(globalschema)
        links = self.order_links(globalschema)
        linkprops = self.order_link_properties(globalschema)
        computables = self.order_computables(globalschema)
        atoms = self.order_atoms(globalschema)

        for atom in atoms:
            if self.include_builtin or atom.name.module != 'semantix.caos.builtins':
                atom.setdefaults()
                self.finalschema.add(atom)

        for comp in computables:
            if self.include_builtin or comp.name.module != 'semantix.caos.builtins':
                comp.setdefaults()
                self.finalschema.add(comp)

        for prop in linkprops:
            if self.include_builtin or prop.name.module != 'semantix.caos.builtins':
                prop.setdefaults()
                self.finalschema.add(prop)

        for link in links:
            if self.include_builtin or link.name.module != 'semantix.caos.builtins':
                link.setdefaults()
                self.finalschema.add(link)

        for link in links:
            if self.include_builtin or link.name.module != 'semantix.caos.builtins':
                link.materialize(self.finalschema)

        for concept in concepts:
            if self.include_builtin or concept.name.module != 'semantix.caos.builtins':
                concept.setdefaults()
                self.finalschema.add(concept)

        for concept in concepts:
            if self.include_builtin or concept.name.module != 'semantix.caos.builtins':
                concept.materialize(self.finalschema)


    def get_proto_schema_class(self, builtin):
        return proto.BuiltinRealmMeta if builtin else proto.RealmMeta


    def get_schema_name_class(self):
        return caos.Name


    def _check_base(self, element, base_name, globalmeta):
        base = globalmeta.get(base_name, type=element.__class__.get_canonical_class(),
                              include_pyobjects=True)
        if isinstance(base, caos.types.ProtoObject) and base.is_final:
            context = lang_context.SourceContext.from_object(element)
            raise MetaError('"%s" is final and cannot be inherited from' % base.name,
                            context=context)


    def read_atoms(self, data, globalmeta, localmeta):
        backend = None

        for atom_name, atom in data['atoms'].items():
            atom.name = caos.Name(name=atom_name, module=self.module)
            atom.backend = backend
            globalmeta.add(atom)
            localmeta.add(atom)

        ns = localmeta.get_namespace(proto.Atom)

        for atom in localmeta('atom', include_builtin=self.include_builtin):
            if atom.base:
                try:
                    atom.base = ns.normalize_name(atom.base, include_pyobjects=True)
                    self._check_base(atom, atom.base, globalmeta)
                except caos.MetaError as e:
                    context = lang_context.SourceContext.from_object(atom)
                    raise MetaError(e, context=context) from e


    def order_atoms(self, globalmeta):
        g = {}

        for atom in globalmeta('atom', include_automatic=True, include_builtin=True):
            constraints = getattr(atom, '_constraints', None)
            if constraints:
                atom.normalize_constraints(globalmeta, constraints)
                for constraint in constraints:
                    atom.add_constraint(constraint)

            g[atom.name] = {"item": atom, "merge": [], "deps": []}

            if atom.base:
                atom_base = globalmeta.get(atom.base, include_pyobjects=True)
                if isinstance(atom_base, proto.Atom):
                    atom.base = atom_base.name
                    g[atom.name]['merge'].append(atom.base)

        return topological.normalize(g, merger=proto.Atom.merge)

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

    def read_link_properties(self, data, globalmeta, localmeta):
        linkprop_ns = localmeta.get_namespace(proto.LinkProperty)

        for property_name, property in data['link-properties'].items():
            module = self.module
            property.name = caos.Name(name=property_name, module=module)

            globalmeta.add(property)
            localmeta.add(property)

        for prop in localmeta('link_property', include_builtin=self.include_builtin):
            if prop.base:
                prop.base = tuple(linkprop_ns.normalize_name(b) for b in prop.base)
            elif prop.name != 'semantix.caos.builtins.link_property':
                prop.base = (caos.Name('semantix.caos.builtins.link_property'),)


    def order_link_properties(self, globalmeta):
        g = {}

        for prop in globalmeta('link_property', include_automatic=True, include_builtin=True):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}

            if prop.base:
                g[prop.name]['merge'].extend(prop.base)

        return topological.normalize(g, merger=proto.LinkProperty.merge)


    def read_properties_for_link(self, link, globalmeta, localmeta):
        atom_ns = localmeta.get_namespace(proto.Atom)
        linkprop_ns = localmeta.get_namespace(proto.LinkProperty)

        props = getattr(link, '_properties', None)
        if not props:
            return

        for property_name, property in props.items():

            property_qname = linkprop_ns.normalize_name(property_name, default=None)

            if not property_qname:
                if not link.generic():
                    # Only generic links can implicitly define properties
                    raise caos.MetaError('reference to an undefined property "%s"' % property_name)

                # The link property has not been defined globally.
                if not caos.Name.is_qualified(property_name):
                    # If the name is not fully qualified, assume inline link property
                    # definition. The only attribute that is used for global definition
                    # is the name.
                    property_qname = caos.Name(name=property_name, module=self.module)
                    propdef = proto.LinkProperty(name=property_qname,
                                    base=(caos.Name('semantix.caos.builtins.link_property'),))
                    globalmeta.add(propdef)
                    localmeta.add(propdef)
                else:
                    property_qname = caos.Name(property_name)

            if link.generic():
                property.target = atom_ns.normalize_name(property.target)
            else:
                link_base = globalmeta.get(link.base[0], type=proto.Link)
                propdef = link_base.pointers.get(property_qname)
                if not propdef:
                    raise caos.MetaError('link "%s" does not define property "%s"' \
                                         % (link.name, property_qname))
                property_qname = propdef.normal_name()

            # A new specialized subclass of the link property is created for each
            # (source, property_name, target_atom) combination
            property.base = (property_qname,)
            prop_genname = proto.LinkProperty.generate_name(link.name, property.target,
                                                            property_qname)
            property.name = caos.Name(name=prop_genname, module=property_qname.module)
            property.source = link

            self.add_pointer_constraints(property, getattr(property, '_constraints', ()), 'atom')
            self.add_pointer_constraints(property, getattr(property, '_abstract_constraints', ()),
                                                                     'atom', 'abstract')

            globalmeta.add(property)
            localmeta.add(property)

            link.add_pointer(property)

    def _create_base_link(self, link, link_qname, globalmeta, localmeta, type=None):
        type = type or proto.Link

        base = 'semantix.caos.builtins.link' if type is proto.Link else \
               'semantix.caos.builtins.link_property'

        linkdef = type(name=link_qname,
                       base=(caos.Name(base),),
                       _setdefaults_=False)

        globalmeta.add(linkdef)
        if localmeta:
            localmeta.add(linkdef)
        return linkdef

    def _read_computables(self, source, globalmeta, localmeta):
        comp_ns = localmeta.get_namespace(proto.Computable)

        for cname, computable in getattr(source, '_computables', {}).items():
            computable_qname = comp_ns.normalize_name(cname, default=None)

            if not computable_qname:
                if not caos.Name.is_qualified(cname):
                    computable_qname = caos.Name(name=cname, module=self.module)
                else:
                    computable_qname = caos.Name(cname)

            if computable_qname in source.own_pointers:
                context = lang_context.SourceContext.from_object(computable)
                raise MetaError('computable "%(name)s" conflicts with "%(name)s" pointer '
                                'defined in the same source' % {'name': computable_qname},
                                 context=context)

            computable_name = proto.Computable.generate_name(source.name, None, computable_qname.name)
            computable.source = source
            computable.name = caos.Name(name=computable_name, module=computable_qname.module)
            computable.setdefaults()
            source.add_pointer(computable)

            super = globalmeta.get(computable.normal_name(), default=None)
            if super is None:
                type = proto.Link if isinstance(source, proto.Concept) else proto.LinkProperty
                super = self._create_base_link(computable, computable.normal_name(), globalmeta,
                                               localmeta, type=type)

            computable.base = (super.name,)

            globalmeta.add(computable)
            localmeta.add(computable)

    def order_computables(self, globalmeta):
        return globalmeta('computable', include_automatic=True, include_builtin=True)


    def read_links(self, data, globalmeta, localmeta):

        link_ns = localmeta.get_namespace(proto.Link)

        for link_name, link in data['links'].items():
            module = self.module
            link.name = caos.Name(name=link_name, module=module)

            self.read_properties_for_link(link, globalmeta, localmeta)

            globalmeta.add(link)
            localmeta.add(link)

        for link in localmeta('link', include_builtin=self.include_builtin):
            if link.base:
                link.base = tuple(link_ns.normalize_name(b) for b in link.base)
            elif link.name != 'semantix.caos.builtins.link':
                link.base = (caos.Name('semantix.caos.builtins.link'),)

            self._read_computables(link, globalmeta, localmeta)

            for index in link._indexes:
                expr, tree = self.normalize_index_expr(index.expr, link, globalmeta, localmeta)
                idx = proto.SourceIndex(expr, tree=tree)
                context = lang_context.SourceContext.from_object(index)
                lang_context.SourceContext.register_object(idx, context)
                link.add_index(idx)

    def order_links(self, globalmeta):
        g = {}

        for link in globalmeta('link', include_automatic=True, include_builtin=True):
            for property_name, property in link.pointers.items():
                if property.target:
                    property.target = globalmeta.get(property.target)

                    constraints = getattr(property, 'constraints', None)
                    if constraints:
                        atom_constraints = [c for c in constraints.values()
                                            if isinstance(c, proto.AtomConstraint)]
                    else:
                        atom_constraints = None
                    if atom_constraints:
                        # Got an inline atom definition.
                        atom = self.genatom(globalmeta, link, property.target.name, property_name,
                                            constraints=atom_constraints, default=property.default)
                        globalmeta.add(atom)
                        property.target = atom

            if link.source and not isinstance(link.source, proto.Prototype):
                link.source = globalmeta.get(link.source)

            if link.target and not isinstance(link.target, proto.Prototype):
                link.target = globalmeta.get(link.target)

            if link.target:
                link.is_atom = isinstance(link.target, proto.Atom)

            g[link.name] = {"item": link, "merge": [], "deps": []}

            if not link.generic() and not link.atomic():
                base = globalmeta.get(link.normal_name())
                if [l for l in base.children() if not l.generic() and l.atomic()]:
                    context = lang_context.SourceContext.from_object(link)
                    raise MetaError('%s link target conflict (atom/concept)' % link.normal_name(),
                                    context=context)

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
                    self._check_base(link, base_name, globalmeta)

                g[link.name]['merge'].extend(link.base)

        try:
            links = topological.normalize(g, merger=proto.Link.merge)
        except caos.MetaError as e:
            if e.context:
                raise MetaError(e.msg, hint=e.hint, details=e.details, context=e.context.context) from e
            raise

        csql_expr = caosql_expr.CaosQLExpression(globalmeta)

        try:
            for link in links:
                for index in link.indexes:
                    csql_expr.check_source_atomic_expr(index.tree, link)

                self.normalize_computables(link, globalmeta)
                self.normalize_pointer_defaults(link, globalmeta)

        except caosql_exc.CaosQLReferenceError as e:
            context = lang_context.SourceContext.from_object(index)
            raise MetaError(e.args[0], context=context) from e

        return links

    def read_concepts(self, data, globalmeta, localmeta):
        backend = None

        concept_ns = localmeta.get_namespace(proto.Concept)
        link_ns = localmeta.get_namespace(proto.Link)

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
                    base_name = concept_ns.normalize_name(b, include_pyobjects=True)
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

            for link_name, link in concept._links.items():
                link = link.link
                link_qname = link_ns.normalize_name(link_name, default=None)
                if not link_qname:
                    # The link has not been defined globally.
                    if not caos.Name.is_qualified(link_name):
                        # If the name is not fully qualified, assume inline link definition.
                        # The only attribute that is used for global definition is the name.
                        link_qname = caos.Name(name=link_name, module=self.module)
                        self._create_base_link(link, link_qname, globalmeta, localmeta)
                    else:
                        link_qname = caos.Name(link_name)

                link.source = concept.name
                link._targets = [concept_ns.normalize_name(t) for t in link._targets]
                link.target = self._normalize_link_target_name(link_qname, link._targets,
                                                               globalmeta, localmeta)

                atom_constraints = self._get_link_atom_constraints(link)

                if atom_constraints:
                    target_name = Atom.gen_atom_name(concept, link_qname)
                    target_name = caos.Name(name=target_name, module=concept.name.module)
                else:
                    target_name = link.target

                # A new specialized subclass of the link is created for each
                # (source, link_name, target) combination
                link.base = (link_qname,)
                link_genname = proto.Link.generate_name(link.source, target_name, link_qname)
                link.name = caos.Name(name=link_genname, module=link_qname.module)

                self.read_properties_for_link(link, globalmeta, localmeta)

                globalmeta.add(link)
                localmeta.add(link)
                concept.add_pointer(link)

        for concept in localmeta('concept', include_builtin=self.include_builtin):
            for link_name, link in concept._links.items():
                link = link.link
                if len(link._targets) > 1:
                    self._create_link_target(concept, link, globalmeta, localmeta)

        for concept in localmeta('concept', include_builtin=self.include_builtin):
            self._read_computables(concept, globalmeta, localmeta)

            for index in getattr(concept, '_indexes', ()):
                expr, tree = self.normalize_index_expr(index.expr, concept, globalmeta, localmeta)
                index.expr = expr
                index.tree = tree
                concept.add_index(index)


    def _create_link_target(self, source, pointer, globalmeta, localmeta):
        targets = [localmeta.get(t, type=proto.Concept) for t in pointer._targets]

        target = pointer.get_common_target(globalmeta, targets)

        existing = globalmeta.get(pointer.target, default=None, type=proto.Concept)
        if not existing:
            localmeta.add(target)
            globalmeta.add(target)


    def _normalize_link_target_name(self, link_fqname, targets, globalmeta, localmeta):
        if len(targets) == 1:
            return targets[0]
        else:
            return proto.Source.gen_virt_parent_name(targets, module=link_fqname.module)


    def normalize_index_expr(self, expr, concept, globalmeta, localmeta):
        expr, tree = self.caosql_expr.normalize_source_expr(expr, concept)
        return expr, tree


    def normalize_pointer_defaults(self, source, globalmeta):
        for link in source.pointers.values():
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
                                            first.issubclass(globalmeta, link.target):
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


    def normalize_computables(self, source, globalmeta):
        for link in source.pointers.values():
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


    def order_concepts(self, globalmeta):
        g = {}

        for concept in globalmeta('concept', include_builtin=True):
            link_target_types = {}

            for link_name, link in concept.pointers.items():
                if not isinstance(link.source, proto.Prototype):
                    link.source = globalmeta.get(link.source)

                if not isinstance(link, proto.Computable):
                    if not isinstance(link.target, proto.Prototype):
                        link.target = globalmeta.get(link.target)
                        if isinstance(link.target, caos.types.ProtoConcept):
                            link.target.add_rlink(link)

                    if isinstance(link.target, proto.Atom):
                        link.is_atom = True

                        parent = globalmeta.get(link.normal_name())

                        if [l for l in parent.children() if not l.generic() and not l.atomic()]:
                            link_context = lang_context.SourceContext.from_object(link)
                            raise MetaError('%s link target conflict (atom/concept)' % link.normal_name(),
                                            context=link_context)

                        if link_name in link_target_types and link_target_types[link_name] != 'atom':
                            raise caos.MetaError('%s link is already defined as a link to non-atom')

                        atom_constraints = self._get_link_atom_constraints(link)

                        if atom_constraints:
                            # Got an inline atom definition.
                            atom = self.genatom(globalmeta, concept, link.target.name, link_name,
                                                constraints=atom_constraints,
                                                default=link.default)
                            globalmeta.add(atom)
                            link.target = atom

                        if link.mapping and link.mapping != caos.types.OneToOne:
                            raise caos.MetaError('%s: links to atoms can only have a "1 to 1" mapping'
                                                 % link_name)

                        link_target_types[link_name] = 'atom'
                    else:
                        if link_name in link_target_types and link_target_types[link_name] == 'atom':
                            link_context = lang_context.SourceContext.from_object(link)
                            raise MetaError('%s link target conflict (atom/concept)' % link.normal_name(),
                                            context=link_context)

                        link_target_types[link_name] = 'concept'

            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                for base_name in concept.base:
                    self._check_base(concept, base_name, globalmeta)
                g[concept.name]["merge"].extend(concept.base)

        concepts = topological.normalize(g, merger=proto.Concept.merge)

        csql_expr = caosql_expr.CaosQLExpression(globalmeta)

        try:
            for concept in concepts:
                for index in concept.indexes:
                    csql_expr.check_source_atomic_expr(index.tree, concept)

                self.normalize_pointer_defaults(concept, globalmeta)
                self.normalize_computables(concept, globalmeta)

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
                          base=base, automatic=True, backend=None, default=default)
        atom.normalize_constraints(meta, constraints)
        for constraint in constraints:
            atom.add_constraint(constraint)
        return atom


class EntityShell(LangObject, adapts=caos.concept.EntityShell):
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
                        else:
                            links[link_name].append(item)
                else:
                    links[link_name] = linkval

            self.entity = session.schema.get(concept, aliases=aliases)(**links)
            for (link_name, target), link_properties in props.items():
                linkcls = caos.concept.getlink(self.entity, link_name, target)
                linkcls.update(**link_properties)

            ent_context.document.import_context.entities.append(self.entity)


class RealmMeta(LangObject, adapts=proto.RealmMeta):
    @classmethod
    def __sx_getstate__(cls, data):
        result = {'atoms': {}, 'links': {}, 'concepts': {}, 'link-properties': {}}

        for type in ('atom', 'link', 'concept', 'link_property'):
            for obj in data(type=type, include_builtin=False, include_automatic=False):
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
        else:
            self.metadata = self.load_from_string(data)

        modhash = persistent_hash(self.metadata._module_)

        repo = deltarepo(module=self.metadata._module_, id=modhash)
        super().__init__(repo)

    def load_from_string(self, data):
        import_context = proto.ImportContext('<string>')
        module = ModuleFromData('<string>')
        context = lang_context.DocumentContext(module=module, import_context=import_context)
        for k, v in lang.yaml.Language.load_dict(io.StringIO(data), context):
            setattr(module, str(k), v)

        return module

    def getmeta(self):
        return self.metadata._index_

    def dump_meta(self, meta):
        prologue = '%SCHEMA semantix.caos.backends.yaml.schemas.Semantics\n---\n'
        return prologue + yaml.Language.dump(meta)
