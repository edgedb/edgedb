##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import io

import importlib
import builtins
import collections
import itertools
import sys
import re

import yaml as pyyaml

import importkit
from importkit import context as lang_context
from importkit import yaml
from importkit.import_ import get_object

from metamagic.utils import lang
from metamagic.utils.lang.yaml.struct import MixedStructMeta
from metamagic.utils.nlang import morphology
from metamagic.utils.algos.persistent_hash import persistent_hash
from metamagic.utils.algos import topological
from metamagic.utils.datastructures import xvalue, OrderedSet, typed

from metamagic import caos
from metamagic.caos import protoschema as lang_protoschema
from metamagic.caos import proto
from metamagic.caos import types as caos_types
from metamagic.caos import backends
from metamagic.caos import delta as base_delta
from metamagic.caos import objects
from metamagic.caos import schema as caos_schema
from metamagic.caos.caosql import errors as caosql_exc
from metamagic.caos.caosql import utils as caosql_utils

from . import delta
from . import protoschema as yaml_protoschema


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


class ImportContext(yaml.Object, adapts=proto.ImportContext):
    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class Bool(yaml.Object, adapts=objects.boolean.Bool, ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return bool(data)


class TimedeltaRepresenter(pyyaml.representer.SafeRepresenter):
    def represent_timedelta(self, data):
        value = str(data)
        return self.represent_scalar(
            'tag:metamagic.sprymix.com,2009/metamagic/timedelta', value)

pyyaml.representer.SafeRepresenter.add_representer(objects.datetime.TimeDelta,
    TimedeltaRepresenter.represent_timedelta)


class TimedeltaConstructor(pyyaml.constructor.Constructor):
    def construct_timedelta(self, data):
        value = self.construct_scalar(node)
        return objects.datetime.TimeDelta(value)

pyyaml.constructor.Constructor.add_constructor(
    'tag:metamagic.sprymix.com,2009/metamagic/timedelta',
    TimedeltaConstructor.construct_timedelta)


class Int(yaml.Object, adapts=objects.int.Int, ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return int(data)


class TypedCollectionMeta(type(yaml.Object), type(typed.AbstractTypedCollection)):
    def __new__(mcls, name, bases, dct, *, adapts=None, ignore_aliases=False, type=None):
        builtins.type(typed.TypedSet).__new__(mcls, name, bases, dct, type=type)
        result = builtins.type(yaml.Object).__new__(mcls, name, bases, dct,
                                                    adapts=adapts, ignore_aliases=ignore_aliases,
                                                    type=type)
        return result

    def __init__(cls, name, bases, dct, *, adapts=None, ignore_aliases=False, type=None):
        builtins.type(typed.TypedSet).__init__(cls, name, bases, dct, type=type)
        builtins.type(yaml.Object).__init__(cls, name, bases, dct,
                                                 adapts=adapts, ignore_aliases=ignore_aliases)


class AbstractTypedCollection(yaml.Object, metaclass=TypedCollectionMeta,
                              adapts=typed.AbstractTypedCollection, type=object):
    _TYPE_ARGS = ('type',)

    def __sx_setstate__(self, data):
        raise NotImplementedError

    @classmethod
    def __sx_getstate__(cls, data):
        raise NotImplementedError


class PrototypeDict(AbstractTypedCollection, adapts=proto.PrototypeDict,
                                             type=proto.BasePrototype):
    def __sx_setstate__(self, data):
        proto.PrototypeDict.__init__(self, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return dict(data.items())


class ArgDict(AbstractTypedCollection, adapts=proto.ArgDict,
                                       type=object):
    def __sx_setstate__(self, data):
        proto.ArgDict.__init__(self, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return dict(data.items())


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


class ExpressionText(StrLangObject, adapts=caos.types.ExpressionText,
                                    ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return pyyaml.ScalarNode('!expr', str(data), style='literal')


class LinkMapping(StrLangObject, adapts=caos.types.LinkMapping, ignore_aliases=True):
    def __new__(cls, data):
        return caos.types.LinkMapping.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class LinkExposedBehaviour(StrLangObject, adapts=caos.types.LinkExposedBehaviour,
                                          ignore_aliases=True):
    def __new__(cls, data):
        return caos.types.LinkExposedBehaviour.__new__(cls, data)

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


class SchemaTypeConstraintSet(AbstractTypedCollection, adapts=proto.SchemaTypeConstraintSet,
                                                       type=proto.SchemaTypeConstraint):
    def __sx_setstate__(self, data):
        if not isinstance(data, list):
            data = (data,)

        constrs = set()

        for constr_type, constr_data in data:
            if constr_type == 'enum':
                constr = proto.SchemaTypeConstraintEnum(data=constr_data)
            else:
                context = lang_context.SourceContext.from_object(self)
                msg = 'invalid schema type constraint type: {!r}'.format(constr_type)
                raise MetaError(msg, context=context)

            constrs.add(constr)

        proto.SchemaTypeConstraintSet.__init__(self, constrs)

    @classmethod
    def __sx_getstate__(cls, data):
        items = []

        for item in data:
            if not hasattr(item, '__sx_getstate__'):
                adapter = yaml.ObjectMeta.get_adapter(item.__class__)
            else:
                adapter = item

            item = adapter.__sx_getstate__(item)
            items.append(item)

        return yaml.types.multimap(items)


class SchemaTypeConstraintEnum(LangObject, adapts=proto.SchemaTypeConstraintEnum):
    _name = 'enum'

    @classmethod
    def __sx_getstate__(cls, data):
        return [cls._name, list(data.data)]


class SchemaType(LangObject, adapts=proto.SchemaType, ignore_aliases=True):
    _typename_re = re.compile(r'(?P<typename>\w+)(?:\((?P<elname>\w+)\))?')

    def __sx_setstate__(self, data):
        if isinstance(data, str):
            typename = data
            constraints = None
        else:
            typename, info = next(iter(data.items()))
            constraints = info.get('constraints')

            if not isinstance(constraints, SchemaTypeConstraintSet):
                # Data originating from delta dump lacks tags to construct the constraint
                # set, so do this manually here.
                c = SchemaTypeConstraintSet.__new__(SchemaTypeConstraintSet)
                c.__sx_setstate__(constraints)
                constraints = c

        m = self._typename_re.match(typename)
        if not m:
            context = lang_context.SourceContext.from_object(self)
            raise MetaError('malformed schema type name: {!r}'.format(typename), context=context)

        main_type = m.group('typename')
        element_type = m.group('elname')

        proto.SchemaType.__init__(self, main_type=main_type, element_type=element_type,
                                        constraints=constraints)

    @classmethod
    def __sx_getstate__(cls, data):
        typename = data.main_type

        if data.element_type:
            typename += '({})'.format(data.element_type)

        if data.constraints:
            return {typename: dict(constraints=data.constraints)}
        else:
            return typename


class PrototypeOrNativeClassRefList(AbstractTypedCollection,
                                    adapts=proto.PrototypeOrNativeClassRefList,
                                    type=proto.PointerCascadeAction):
    @classmethod
    def __sx_getstate__(cls, data):
        result = []

        for item in data:
            if isinstance(item, proto.Prototype):
                item = item.name
            result.append(item)

        return result


class PrototypeMeta(LangObjectMeta, MixedStructMeta):
    pass


class Prototype(LangObject, adapts=proto.Prototype, metaclass=PrototypeMeta):
    pass


class DefaultSpecList(AbstractTypedCollection, adapts=proto.DefaultSpecList,
                      type=object):
    def __sx_setstate__(self, data):
        context = lang_context.SourceContext.from_object(self)

        if data is None:
            default = []
        elif not isinstance(data, list):
            default = [data]
        else:
            default = data

        for i, element in enumerate(default):
            if isinstance(element, dict):
                # backwards compatibility
                element = ExpressionText(element['query'])
                default[i] = element

            if isinstance(element, ExpressionText):
                lang_context.SourceContext.register_object(
                    element, context)

        proto.DefaultSpecList.__init__(self, default)

    @classmethod
    def __sx_getstate__(cls, data):
        return list(data)


class Attribute(LangObject, adapts=proto.Attribute):
    def __sx_setstate__(self, data):
        proto.Attribute.__init__(self, name=None, title=data['title'],
                                 description=data.get('description'),
                                 type=data.get('type'),
                                 _setdefaults_=False, _relaxrequired_=True)

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        result['type'] = data.type

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        return result


class AttributeValue(LangObject, ignore_aliases=True, adapts=proto.AttributeValue):
    def __sx_setstate__(self, data):
        proto.AttributeValue.__init__(self, value=data, _setdefaults_=False, _relaxrequired_=True)

    @classmethod
    def __sx_getstate__(cls, data):
        return data.value


class AtomConstraint(LangObject, ignore_aliases=True):
    pass


class Constraint(LangObject, adapts=proto.Constraint):
    def __sx_setstate__(self, data):
        if isinstance(data, dict):
            extends = data.get('extends')
            if extends:
                if not isinstance(extends, list):
                    extends = [extends]

            proto.Constraint.__init__(self, title=data['title'], description=data['description'],
                                            is_abstract=data.get('abstract'),
                                            is_final=data.get('final'),
                                            errmessage=data.get('errmessage'),
                                            _setdefaults_=False, _relaxrequired_=True)

            self._bases = extends
            self._expr = data.get('expr')
            self._subjectexpr = data.get('subject')
            self._paramtypes = data.get('paramtypes')
            _params = data.get('params') or {}
        else:
            proto.Constraint.__init__(self, _setdefaults_=False, _relaxrequired_=True)
            self._expr = None
            _params = dict(param=data)

        self._params = {}

        for p, v in _params.items():
            if isinstance(v, str):
                v = v.strip(' \n')
            self._params[p] = v

        self._yml_workattrs = {'_bases', '_expr', '_subjectexpr', '_paramtypes', '_param'}

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.generic():
            if data.bases:
                result['extends'] = [b.name for b in data.bases]

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.is_abstract:
            result['abstract'] = data.is_abstract

        if data.is_final:
            result['final'] = data.is_final

        if data.expr:
            result['expr'] = data.expr

        if data.errmessage:
            result['errmessage'] = data.errmessage

        if data.subjectexpr:
            result['subject'] = data.subjectexpr

        if data.paramtypes:
            result['paramtypes'] = data.paramtypes

        return result


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


class AtomConstraintExpr(AtomConstraint, adapts=proto.AtomConstraintExpr):
    def __sx_setstate__(self, data):
        proto.AtomConstraintExpr.__init__(self, [data['expr'].strip(' \n')])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'expr': next(iter(data.values))}


class AtomConstraintEnum(AtomConstraint, adapts=proto.AtomConstraintEnum):
    def __sx_setstate__(self, data):
        proto.AtomConstraintEnum.__init__(self, data['enum'])

    @classmethod
    def __sx_getstate__(cls, data):
        return {'enum': list(data.values)}


class AtomConstraintRegExp(AtomConstraint, adapts=proto.AtomConstraintRegExp):
    def __sx_setstate__(self, data):
        proto.AtomConstraintRegExp.__init__(self, [data['regexp']])

    @classmethod
    def __sx_getstate__(self, data):
        return {'regexp': next(iter(data.values))}

default_name = None

class Atom(Prototype, adapts=proto.Atom):
    def __sx_setstate__(self, data):
        proto.Atom.__init__(self, name=default_name,
                            default=data['default'], title=data['title'],
                            description=data['description'],
                            is_abstract=data['abstract'],
                            is_final=data['final'],
                            _setdefaults_=False, _relaxrequired_=True)
        self._attributes = data.get('attributes')
        self._constraints = data.get('constraints')
        self._bases = [data['extends']]
        self._yml_workattrs = {'_constraints', '_bases', '_attributes'}

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.bases:
            extends = []

            for b in data.bases:
                try:
                    extends.append(b.name)
                except AttributeError:
                    extends.append(b.class_name)
            result['extends'] = extends

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

        if data.local_constraints:
            result['constraints'] = dict(data.local_constraints)

        if data.local_attributes:
            result['attributes'] = dict(data.local_attributes)

        return result


class Concept(Prototype, adapts=proto.Concept):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.Concept.__init__(self, name=default_name,
                               title=data.get('title'), description=data.get('description'),
                               is_abstract=data.get('abstract'), is_final=data.get('final'),
                               _setdefaults_=False, _relaxrequired_=True)
        self._bases = extends
        self._links = data.get('links', {})
        self._indexes = data.get('indexes') or ()
        self._constraints = data.get('constraints')
        self._abstract_constraints = data.get('abstract-constraints')
        self._yml_workattrs = {'_links', '_indexes', '_bases', '_constraints',
                               '_abstract_constraints'}

    @classmethod
    def __sx_getstate__(cls, data):
        result = {
            'extends': list(itertools.chain((b.name for b in data.bases),
                                            (b.class_name for b in data.custombases)))
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
            for ptr_name, ptr in data.own_pointers.items():
                if isinstance(ptr.target, proto.Concept) and ptr.target.is_virtual:
                    # key = tuple(t.name for t in ptr.target._virtual_children)
                    key = 'virtual___'
                else:
                    key = ptr.target.name

                result['links'][ptr_name] = {key: ptr}

        if data.indexes:
            result['indexes'] = list(sorted(data.indexes, key=lambda i: i.expr))

        if data.local_constraints:
            result['constraints'] = dict(data.local_constraints)

        return result

    def process_index_expr(self, index):
        return index

    def finalize(self, meta):
        indexes = set()
        for index in self.indexes:
            indexes.add(self.process_index_expr(index))
        self.indexes = indexes

        proto.Concept.finalize(self, meta)


class SourceIndex(LangObject, adapts=proto.SourceIndex, ignore_aliases=True):
    def __sx_setstate__(self, data):
        proto.SourceIndex.__init__(self, expr=data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data.expr)


class PointerCascadeAction(LangObject, adapts=proto.PointerCascadeAction):
    def __sx_setstate__(self, data):
        proto.PointerCascadeAction.__init__(self, name=default_name, title=data['title'],
                                            description=data.get('description'),
                                            _setdefaults_=False, _relaxrequired_=True)

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        return result


class PointerCascadeActionSet(AbstractTypedCollection,
                              adapts=proto.PointerCascadeActionSet,
                              type=proto.PointerCascadeAction):
    def __sx_setstate__(self, data):
        if not isinstance(data, list):
            data = (data,)

        items = []
        for name in data:
            item = proto.PointerCascadeAction(_setdefaults_=False,
                                              _relaxrequired_=True)
            item._name = name
            items.append(item)

        proto.PointerCascadeActionSet.__init__(self, items)

    @classmethod
    def __sx_getstate__(cls, data):
        # prototype_name here is for PrototypeRefs, which will
        # appear in partial schema loads, such as with delta loads.
        return [getattr(item, 'prototype_name', item.name) for item in data]


class PointerCascadeEvent(LangObject, adapts=proto.PointerCascadeEvent):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.PointerCascadeEvent.__init__(self, name=default_name, title=data['title'],
                                           description=data['description'],
                                           allowed_actions=data['allowed-actions'],
                                           _setdefaults_=False, _relaxrequired_=True)
        self._bases = extends
        self._yml_workattrs = {'_bases'}

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.bases:
            result['extends'] = data.bases[0].name

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        result['allowed-actions'] = list(sorted(a.name for a in data.allowed_actions))

        return result


class PointerCascadePolicy(LangObject, adapts=proto.PointerCascadePolicy):
    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        result['name'] = data.name
        result['subject'] = data.subject.name
        result['event'] = data.event.name
        result['action'] = data.action.name

        return result


class LinkPropertyDef(Prototype, proto.LinkProperty):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.LinkProperty.__init__(self, name=default_name, title=data['title'],
                                    description=data['description'], readonly=data['readonly'],
                                    loading=data['loading'], required=data['required'],
                                    _setdefaults_=False, _relaxrequired_=True)

        self._bases = extends
        self._yml_workattrs = {'_bases'}

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.generic():
            if data.bases:
                result['extends'] = [b.name for b in data.bases]

        if data.title:
            result['title'] = data.title

        if data.description:
            result['description'] = data.description

        if data.readonly:
            result['readonly'] = data.readonly

        if data.loading:
            result['loading'] = data.loading

        if data.required:
            result['required'] = data.required

        return result


class LinkProperty(Prototype, adapts=proto.LinkProperty, ignore_aliases=True):
    def __sx_setstate__(self, data):
        if isinstance(data, ExpressionText):
            context = lang_context.SourceContext.from_object(self)
            lang_context.SourceContext.register_object(data, context)
            proto.LinkProperty.__init__(self, _setdefaults_=False,
                                              _relaxrequired_=True,
                                              default=[data],
                                              readonly=True)

        elif isinstance(data, str):
            proto.LinkProperty.__init__(self, name=default_name,
                                              _setdefaults_=False,
                                              _relaxrequired_=True)
            self._target = data
            self._yml_workattrs = {'_target'}

        else:
            atom_name, info = next(iter(data.items()))

            proto.LinkProperty.__init__(self, name=default_name,
                                        title=info['title'],
                                        description=info['description'],
                                        readonly=info['readonly'],
                                        default=info['default'],
                                        loading=info['loading'],
                                        required=info['required'],
                                        _setdefaults_=False,
                                        _relaxrequired_=True)
            self._constraints = info.get('constraints')
            self._abstract_constraints = info.get('abstract-constraints')
            self._target = atom_name
            self._yml_workattrs = {
                '_constraints', '_abstract_constraints', '_target'
            }

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.local_constraints:
            result['constraints'] = dict(data.local_constraints)

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
        proto.LinkProperty.__init__(self, name=default_name,
                                    default=data['default'],
                                    _setdefaults_=False,
                                    _relaxrequired_=True)


class LinkDef(Prototype, adapts=proto.Link):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        proto.Link.__init__(self, name=default_name,
                            title=data['title'],
                            description=data['description'],
                            is_abstract=data.get('abstract'),
                            is_final=data.get('final'),
                            readonly=data.get('readonly'),
                            mapping=data.get('mapping'),
                            exposed_behaviour=data.get('exposed_behaviour'),
                            loading=data.get('loading'),
                            default=data.get('default'),
                            _setdefaults_=False, _relaxrequired_=True)

        self._bases = extends
        self._properties = data['properties']
        self._indexes = data.get('indexes') or ()
        self._cascades = data.get('cascades')
        self._yml_workattrs = {'_properties', '_indexes', '_cascades', '_bases'}

    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        if data.generic():
            if data.bases:
                result['extends'] = [b.name for b in data.bases]

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

        if data.exposed_behaviour:
            result['exposed_behaviour'] = data.exposed_behaviour

        if data.required:
            result['required'] = data.required

        if data.default is not None:
            result['default'] = data.default

        if data.own_pointers:
            result['properties'] = {}
            for ptr_name, ptr in data.own_pointers.items():
                result['properties'][ptr_name] = ptr

        if data.local_constraints:
            result['constraints'] = dict(data.local_constraints)

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

        if isinstance(data, ExpressionText):
            context = lang_context.SourceContext.from_object(self)
            lang_context.SourceContext.register_object(data, context)
            link = proto.Link(_setdefaults_=False, _relaxrequired_=True,
                              default=[data], readonly=True)
            lang_context.SourceContext.register_object(link, context)
            self.link = link

        elif isinstance(data, (str, list)):
            link = proto.Link(source=None, target=None, name=default_name, _setdefaults_=False,
                              _relaxrequired_=True)
            lang_context.SourceContext.register_object(link, context)
            link._targets = (data,) if isinstance(data, str) else data
            link._yml_workattrs = {'_targets'}
            self.link = link

        elif isinstance(data, dict):
            if len(data) != 1:
                raise MetaError('unexpected number of elements in link data dict: %d', len(data),
                                context=context)

            targets, info = next(iter(data.items()))

            if not isinstance(targets, tuple):
                targets = (targets,)

            context = lang_context.SourceContext.from_object(self)
            props = info['properties']

            link = proto.Link(name=default_name, target=None,
                              mapping=info['mapping'],
                              exposed_behaviour=info['exposed_behaviour'],
                              required=info['required'],
                              title=info['title'],
                              description=info['description'],
                              readonly=info['readonly'],
                              loading=info['loading'],
                              default=info['default'],
                              _setdefaults_=False, _relaxrequired_=True)

            search = info.get('search')
            if search and search.weight is not None:
                link.search = search

            lang_context.SourceContext.register_object(link, context)

            link._constraints = info.get('constraints')
            if link._constraints:
                for constraint in link._constraints:
                    if isinstance(constraint, proto.PointerConstraintUnique):
                        if list(constraint.values)[0] == True:
                            constraint.values = {"self.target"}

            link._abstract_constraints = info.get('abstract-constraints')
            link._properties = props
            link._targets = targets
            link._cascades = info['cascades']

            link._yml_workattrs = {'_constraints', '_abstract_constraints', '_properties',
                                   '_targets', '_cascades'}

            self.link = link
        else:
            raise MetaError('unexpected specialized link format: %s', type(data), context=context)


class ProtoSchemaAdapter(yaml_protoschema.ProtoSchemaAdapter):
    def load_imports(self, context, localschema):
        this_module = self.module.name

        imports = context.document.imports.copy()

        self.module.imports = frozenset(m.__name__ for m in imports.values())

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


    def process_data(self, data, localschema):
        self.collect_atoms(data, localschema)
        self.collect_attributes(data, localschema)

        self.collect_constraints(data, localschema)

        # Constraints have no external dependencies, but need to
        # be fully initialized when we get to constraint users below.
        constraints = self.merge_and_sort_constraints(localschema)

        # Ditto for attributes
        attributes = self.merge_and_sort_attributes(localschema)

        # Atoms depend only on constraints and attributes, can process them now
        atoms = self.merge_and_sort_atoms(localschema)

        self.read_cascade_actions(data, localschema)
        self.read_cascade_events(data, localschema)
        self.read_link_properties(data, localschema)
        self.read_links(data, localschema)
        self.read_concepts(data, localschema)
        self.read_cascade_policy(data, localschema)

        # The final pass on may produce additional objects,
        # thus, it has to be performed in reverse order (mostly).
        concepts = OrderedSet(self.order_concepts(localschema))
        links = OrderedSet(self.order_links(localschema))
        linkprops = OrderedSet(self.order_link_properties(localschema))
        cascade_policy = OrderedSet(self.order_cascade_policy(localschema))
        cascade_events = OrderedSet(self.order_cascade_events(localschema))
        cascade_actions = OrderedSet(self.order_cascade_actions(localschema))
        attribute_values = OrderedSet(self.order_attribute_values(localschema))

        constraints.update(self.collect_derived_constraints(localschema))
        self.finalize_constraints(constraints, localschema)

        for attribute in attributes:
            attribute.setdefaults()

        for attribute_value in attribute_values:
            attribute_value.setdefaults()

        for action in cascade_actions:
            action.setdefaults()

        for event in cascade_events:
            event.setdefaults()

        for policy in cascade_policy:
            policy.setdefaults()

        for atom in atoms:
            atom.setdefaults()
            if hasattr(atom, '_yml_workattrs'):
                for workattr in atom._yml_workattrs:
                    delattr(atom, workattr)
                delattr(atom, '_yml_workattrs')

        for atom in atoms:
            atom.finalize(localschema)

        for prop in linkprops:
            prop.setdefaults()
            if hasattr(prop, '_yml_workattrs'):
                for workattr in prop._yml_workattrs:
                    delattr(prop, workattr)
                delattr(prop, '_yml_workattrs')

        for link in links:
            link.setdefaults()
            if hasattr(link, '_yml_workattrs'):
                for workattr in link._yml_workattrs:
                    delattr(link, workattr)
                delattr(link, '_yml_workattrs')

        for link in links:
            link.finalize(localschema)

        for concept in concepts:
            concept.setdefaults()

            if hasattr(concept, '_yml_workattrs'):
                for workattr in concept._yml_workattrs:
                    delattr(concept, workattr)
                delattr(concept, '_yml_workattrs')

        for concept in concepts:
            try:
                self.normalize_pointer_defaults(concept, localschema)

            except caosql_exc.CaosQLReferenceError as e:
                concept_context = lang_context.SourceContext.from_object(concept)
                raise MetaError(e.args[0], context=concept_context) from e

            concept.finalize(localschema)

        for concept in concepts:
            for index in concept.own_indexes:
                expr = self.normalize_index_expr(index.expr, concept, localschema)
                index.expr = expr

        # Link meterialization might have produced new atoms
        for atom in localschema(type=caos.proto.Atom):
            if atom.name.module == self.module.name:
                atoms.add(atom)

        for constraint in localschema(type=caos.proto.Constraint):
            if constraint.name.module == self.module.name:
                constraints.add(constraint)

        # Link meterialization might have produced new specialized properties
        for prop in localschema(type=caos.proto.LinkProperty):
            if prop.name.module != self.module.name:
                self._add_foreign_proto(prop)
            else:
                linkprops.add(prop)

        # Concept meterialization might have produced new specialized links
        for link in localschema(type=caos.proto.Link):
            if link.name.module != self.module.name:
                self._add_foreign_proto(link)
            else:
                links.add(link)

        for link in localschema(type=caos.proto.Computable):
            if link.name.module != self.module.name:
                self._add_foreign_proto(link)
            else:
                links.add(link)

        # Arrange prototypes in the resulting schema according to determined topological order.
        localschema.reorder(itertools.chain(attributes, attribute_values, cascade_actions,
                                            cascade_events, constraints, atoms, linkprops,
                                            links, concepts, cascade_policy))


    def get_proto_schema_class(self):
        return proto.ProtoSchema


    def get_proto_module_class(self):
        return proto.ProtoModule


    def get_schema_name_class(self):
        return caos.Name


    def _check_base(self, element, base_name, localschema):
        base = localschema.get(base_name, type=element.__class__.get_canonical_class(),
                               include_pyobjects=True, index_only=False)
        if isinstance(base, caos.types.ProtoObject):
            if base.is_final:
                context = lang_context.SourceContext.from_object(element)
                raise MetaError('"%s" is final and cannot be inherited from' % base.name,
                                context=context)
        else:
            # Native class reference
            base = caos.proto.NativeClassRef(class_name='{}.{}'.format(base.__module__,
                                                                       base.__name__))

        return base


    def _merge_and_sort_objects(self, localschema, objtype, objmerger):
        g = {}

        for obj in self.module(objtype):
            g[obj.name] = {"item": obj, "merge": [], "deps": []}

            if obj.bases:
                g[obj.name]['merge'].extend(b.name for b in obj.bases)

                for base in obj.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}

        objs = topological.normalize(g, merger=objmerger, context=localschema)
        return OrderedSet(filter(lambda obj: obj.name.module == self.module.name, objs))


    def _parse_typeref(self, typeref, localschema, context):
        try:
            collection_type, type = proto.TypeRef.parse(typeref)
        except ValueError as e:
            raise MetaError(e.args[0], context=context) from None

        if type is not None:
            try:
                type = localschema.get(type)
            except caos.MetaError as e:
                raise MetaError(e, context=context)

        if collection_type is not None:
            type = collection_type(element_type=type)

        return type

    def collect_attributes(self, data, localschema):
        for attribute_name, attribute in data['attributes'].items():
            attribute.name = caos.Name(name=attribute_name, module=self.module.name)
            self._add_proto(localschema, attribute)


    def merge_and_sort_attributes(self, localschema):
        return OrderedSet(self.module('attribute'))


    def order_attribute_values(self, localschema):
        return self.module('attribute-value')


    def read_cascade_actions(self, data, localschema):
        for action_name, action in data['cascade-actions'].items():
            action.name = caos.Name(name=action_name, module=self.module.name)
            self._add_proto(localschema, action)


    def order_cascade_actions(self, localschema):
        return self.module('cascade-action')


    def read_cascade_events(self, data, localschema):
        for event_name, event in data['cascade-events'].items():
            event.name = caos.Name(name=event_name, module=self.module.name)

            actions = proto.PointerCascadeActionSet()
            for action in event.allowed_actions:
                action = localschema.get(action._name, type=proto.PointerCascadeAction,
                                                       index_only=False)
                actions.add(action)

            event.allowed_actions = actions

            self._add_proto(localschema, event)


    def order_cascade_events(self, localschema):
        g = {}

        for event in self.module('cascade-event'):
            g[event.name] = {"item": event, "merge": [], "deps": []}

            if event._bases:
                bases = []

                for base_name in event._bases:
                    base = self._check_base(event, base_name, localschema)
                    if base_name.module != self.module.name:
                        g[base_name] = {"item": base, "merge": [], "deps": []}
                    bases.append(base)

                event.bases = bases
                g[event.name]["merge"].extend(event._bases)

        atoms = topological.normalize(g, merger=proto.PointerCascadeEvent.merge,
                                      context=localschema)
        return list(filter(lambda a: a.name.module == self.module.name, atoms))


    def _normalize_attribute_values(self, localschema, subject, attributes):
        attrs = {}

        for attribute, attrvalue in attributes.items():
            attribute = localschema.get(attribute, index_only=False)

            name = caos.types.ProtoAttributeValue.generate_specialized_name(subject.name,
                                                                            attribute.name)

            name = caos.name.Name(name=name, module=self.module.name)

            attrvalue.name = name
            attrvalue.subject = subject
            attrvalue.attribute = attribute

            if attribute.type.is_container and not isinstance(attrvalue.value, list):
                val = [attrvalue.value]
            else:
                val = attrvalue.value

            try:
                attrvalue.value = attribute.type.coerce(val)
            except ValueError as e:
                msg = e.args[0].format(name=attribute.name.name)
                context = lang_context.SourceContext.from_object(attrvalue)
                raise MetaError(msg, context=context) from e

            attrs[attribute.name] = attrvalue

            self._add_proto(localschema, attrvalue)

        return attrs


    def _read_cascade_policies(self, pointer, cascade_policies, localschema):
        for event, actions in cascade_policies.items():
            if isinstance(actions, str):
                actions = {'default': actions}

            event = localschema.get(event, index_only=False)

            for category, action in actions.items():
                action = localschema.get(action, index_only=False)

                name = caos.types.ProtoPointerCascadePolicy.generate_name(pointer, event,
                                                                          category)
                name = caos.name.Name(name=name, module=self.module.name)

                policy = proto.PointerCascadePolicy(name=name, subject=pointer, event=event,
                                                    action=action, category=category)

                self._add_proto(localschema, policy)


    def read_cascade_policy(self, data, localschema):
        data = data.get('policies')

        links = data.get('links') if data else None
        if links:
            for link_name, link_data in data['links'].items():
                link = localschema.get(link_name)
                cascade_policies = link_data.get('cascades')
                if cascade_policies:
                    self._read_cascade_policies(link, link_data['cascades'], localschema)

        concepts = data.get('concepts') if data else None
        if concepts:
            for concept_name, concept_data in concepts.items():
                concept = localschema.get(concept_name)

                links = concept_data.get('links')

                if links:
                    for link_name, link_data in links.items():
                        if not caos.Name.is_qualified(link_name):
                            # If the name is not fully qualified, assume inline link definition.
                            # The only attribute that is used for global definition is the name.
                            link_qname = caos.Name(name=link_name, module=self.module.name)
                        else:
                            link_qname = caos.Name(link_name)

                        genlink = localschema.get(link_qname)

                        speclink = concept.pointers[genlink.name]

                        cascade_policies = link_data.get('cascades')
                        if cascade_policies:
                            self._read_cascade_policies(speclink, link_data['cascades'], localschema)


    def order_cascade_policy(self, localschema):
        return self.module('cascade-policy')


    def collect_atoms(self, data, localschema):
        # First pass on atoms.
        #
        # Keeps this very simple and do not attempt to resolve anything
        # besides bases to avoid circular dependency, as atoms are used
        # in attribute and constraint definitions.
        #
        for atom_name, atom in data['atoms'].items():
            atom.name = caos.Name(name=atom_name, module=self.module.name)
            self._add_proto(localschema, atom)

        for atom in self.module('atom'):
            if atom._bases:
                try:
                    atom.bases = [self._check_base(atom, atom._bases[0], localschema)]
                except caos.MetaError as e:
                    context = lang_context.SourceContext.from_object(atom)
                    raise MetaError(e, context=context) from e


    def merge_and_sort_atoms(self, localschema):
        # Second pass on atoms.
        #
        # Resolve all properties and perform inheritance merge.
        #
        context = lang_context.SourceContext.from_object(self)
        this_module = context.document.import_context

        g = {}

        for atom in self.module('atom'):
            this_item = g[atom.name] = {"item": atom, "merge": [], "deps": set()}

            if atom.name.module == this_module:
                attributes = getattr(atom, '_attributes', None)
                if attributes:
                    attrs = self._normalize_attribute_values(localschema, atom, attributes)
                    for attr in attrs.values():
                        atom.add_attribute(attr)

                constraints = getattr(atom, '_constraints', None)
                if constraints:
                    self._collect_constraints_for_subject(atom, constraints, localschema)

                    ptypes = set()
                    for constraint in atom.constraints.values():
                        if constraint.paramtypes:
                            ptypes.update(constraint.paramtypes.values())
                        inferred = constraint.inferredparamtypes
                        if inferred:
                            ptypes.update(inferred.values())

                    ptypes = {p.element_type if isinstance(p, proto.Collection) else p
                                                                    for p in ptypes}

                    # Add dependency on all builtin atoms unconditionally
                    builtins = localschema.get_module('metamagic.caos.builtins')
                    ptypes.update(builtins('atom'))

                    for p in ptypes:
                        if p is not atom:
                            this_item['deps'].add(p.name)
                            g[p.name] = {"item": p, "merge": [], "deps": []}

                if atom.bases:
                    atom_base = atom.bases[0]

                    if isinstance(atom_base, proto.Atom) and atom.name:
                        this_item['merge'].append(atom_base.name)
                        if atom_base.name.module != self.module.name:
                            g[atom_base.name] = {"item": atom_base, "merge": [], "deps": []}

        atoms = topological.normalize(g, merger=proto.Atom.merge, context=localschema)
        return OrderedSet(filter(lambda a: a.name.module == self.module.name, atoms))

    def collect_constraints(self, data, localschema):
        # First pass on constraint definitions.
        #
        # Constraints potentially depend on atoms, so this must be called after
        # collect_atoms().
        #

        cschema = caos_schema.constraints.ConstraintsSchema

        for constraint_name, constraint in data['constraints'].items():
            constraint.name = caos.Name(name=constraint_name, module=self.module.name)
            self._add_proto(localschema, constraint)

        for constraint in self.module('constraint'):
            context = lang_context.SourceContext.from_object(constraint)
            module_aliases = self._get_obj_module_aliases(constraint)

            if constraint._bases:
                try:
                    constraint.bases = [self._check_base(constraint, b, localschema)
                                        for b in constraint._bases]
                except caos.MetaError as e:
                    context = lang_context.SourceContext.from_object(constraint)
                    raise MetaError(e, context=context) from e

            elif constraint.name != 'metamagic.caos.builtins.constraint':
                # All constraints inherit from builtins.constraint
                constraint.bases = [localschema.get('metamagic.caos.builtins.constraint')]
            else:
                constraint.bases = []

            if constraint._paramtypes:
                # Explicit parameter type is given to validate and coerce param
                # values into.

                paramtypes = {}
                for pn, typeref in constraint._paramtypes.items():
                    type = self._parse_typeref(typeref, localschema, context)
                    paramtypes[pn] = type

                constraint.paramtypes = paramtypes

            if constraint._expr:
                # No point in interpreting the expression in generic constraint def,
                # but we still need to do validation.
                #
                try:
                    expr = cschema.normalize_constraint_expr(localschema, module_aliases,
                                                             constraint._expr)
                except (ValueError, caosql_exc.CaosQLQueryError) as e:
                    raise MetaError(e.args[0], context=context) from None

                constraint.expr = expr

            if constraint._subjectexpr:
                # Again, no interpretation, simple validation
                try:
                    expr = cschema.normalize_constraint_subject_expr(localschema, module_aliases,
                                                                     constraint._subjectexpr)
                except (ValueError, caosql_exc.CaosQLQueryError) as e:
                    raise MetaError(e.args[0], context=context) from None

                constraint.subjectexpr = expr


    def merge_and_sort_constraints(self, localschema):
        return self._merge_and_sort_objects(localschema, 'constraint', proto.Constraint.merge)


    def collect_derived_constraints(self, localschema):
        constraints = self.module('constraint')
        return OrderedSet(c for c in constraints if c.subject is not None)


    def finalize_constraints(self, constraints, localschema):
        cschema = caos_schema.constraints.ConstraintsSchema

        for constraint in constraints:
            constraint.acquire_ancestor_inheritance(localschema)
            constraint.setdefaults()

        return constraints


    def _collect_constraints_for_subject(self, subject, constraints,
                                               localschema, abstract=False):
        # Perform initial collection of constraints defined in subject context.
        # At this point all referenced constraints should be fully initialized.

        cschema = caos_schema.constraints.ConstraintsSchema
        namegen = proto.Constraint.generate_specialized_name

        constr = {}

        for constraint_name, constraint in constraints:
            if constraint._expr:
                constraint.expr = constraint._expr

            constraint_base = localschema.get(constraint_name,
                                              type=proto.Constraint)
            constraint_qname = constraint_base.name

            # A new specialized subclass of the constraint is created
            # for each subject referencing the constraint.
            #
            constraint.bases = [localschema.get(constraint_qname,
                                                type=caos.proto.Constraint)]
            constraint.subject = subject
            constraint.acquire_ancestor_inheritance(localschema)

            constr_genname = namegen(subject.name, constraint.bases[0].name)
            constraint.name = caos.Name(name=constr_genname,
                                        module=self.module.name)
            constraint.is_abstract = abstract

            # We now have a full set of data to perform final validation
            # and analysis of the constraint.
            #
            cschema.process_specialized_constraint(localschema, constraint)

            # There can be only one specialized constraint per constraint
            # class per subject. At this point all placeholders have been
            # folded, so it is possible to merge the constraints consistently
            # by merging their final exprs.
            #
            try:
                prev = constr[constraint.bases[0].name]
            except KeyError:
                constr[constraint.bases[0].name] = constraint
            else:
                constraint.merge(prev, context=localschema, local=True)
                constr[constraint.bases[0].name] = constraint

        for c in constr.values():
            # Note that we don't do finalization for the constraint
            # here, since it's possible that it will be further used
            # in a merge of it's subject.
            #
            self._add_proto(localschema, c)
            subject.add_constraint(c)


    def read_link_properties(self, data, localschema):
        for property_name, property in data['link-properties'].items():
            module = self.module.name
            property.name = caos.Name(name=property_name, module=module)

            self._add_proto(localschema, property)

        for prop in self.module('link_property'):
            if prop._bases:
                bases = []
                for base_name in prop._bases:
                    base = self._check_base(prop, base_name, localschema)
                    bases.append(base)
                prop.bases = bases
            elif prop.name != 'metamagic.caos.builtins.link_property':
                prop.bases = [localschema.get('metamagic.caos.builtins.link_property',
                                              type=proto.LinkProperty)]


    def order_link_properties(self, localschema):
        g = {}

        for prop in self.module('link_property'):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}

            if prop.bases:
                g[prop.name]['merge'].extend(pb.name for pb in prop.bases)

                for base in prop.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}

        p = topological.normalize(g, merger=proto.LinkProperty.merge, context=localschema)
        return list(filter(lambda p: p.name.module == self.module.name, p))


    def read_properties_for_link(self, link, localschema):
        props = getattr(link, '_properties', None)
        if not props:
            return

        return self._read_properties_for_link(link, props, localschema)

    def _read_properties_for_link(self, link, props, localschema):
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
                    propdef_base = localschema.get('metamagic.caos.builtins.link_property',
                                                   type=proto.LinkProperty)
                    propdef = proto.LinkProperty(name=property_qname, bases=[propdef_base])
                    self._add_proto(localschema, propdef)
                else:
                    property_qname = caos.Name(property_name)
            else:
                property_qname = property_base.name

            if link.generic() or getattr(property, '_target', None) is not None:
                targetname = getattr(property, '_target', None)
                if targetname:
                    property.target = localschema.get(targetname)
            else:
                link_base = link.bases[0]
                propdef = link_base.pointers.get(property_qname)
                if not propdef:
                    raise caos.MetaError('link "%s" does not define property "%s"' \
                                         % (link.name, property_qname))
                property_qname = propdef.normal_name()

            # A new specialized subclass of the link property is created for each
            # (source, property_name, target_atom) combination
            property.bases = [localschema.get(property_qname, type=caos.proto.LinkProperty)]
            prop_genname = proto.LinkProperty.generate_specialized_name(link.name, property_qname)

            property.name = caos.Name(name=prop_genname, module=self.module.name)
            property.source = link

            constraints = getattr(property, '_constraints', None)
            if constraints:
                self._collect_constraints_for_subject(property, constraints, localschema)

            abstract_constraints = getattr(property, '_abstract_constraints', None)
            if abstract_constraints:
                self._collect_constraints_for_subject(property, abstract_constraints, localschema,
                                                      abstract=True)

            self._add_proto(localschema, property)

            link.add_pointer(property)

    def _create_base_link(self, link, link_qname, localschema, type=None):
        type = type or proto.Link

        base = 'metamagic.caos.builtins.link' if type is proto.Link else \
               'metamagic.caos.builtins.link_property'

        base = localschema.get(base, type=type)
        linkdef = type(name=link_qname, bases=[base], _setdefaults_=False)

        self._add_proto(localschema, linkdef)
        return linkdef

    def read_links(self, data, localschema):
        for link_name, link in data['links'].items():
            module = self.module.name
            link.name = caos.Name(name=link_name, module=module)
            self._add_proto(localschema, link)

        for link in self.module('link'):
            if link._bases:
                bases = []
                for base_name in link._bases:
                    base = self._check_base(link, base_name, localschema)
                    bases.append(base)
                link.bases = bases
            elif link.name != 'metamagic.caos.builtins.link':
                link.bases = [localschema.get('metamagic.caos.builtins.link')]

        for link in self.module('link'):
            self.read_properties_for_link(link, localschema)

            for index in link._indexes:
                expr = self.normalize_index_expr(index.expr, link, localschema)
                idx = proto.SourceIndex(expr)
                context = lang_context.SourceContext.from_object(index)
                lang_context.SourceContext.register_object(idx, context)
                link.add_index(idx)

    def order_links(self, localschema):
        g = {}

        for link in self.module('link'):
            g[link.name] = {"item": link, "merge": [], "deps": []}

            if link.name.module != self.module.name and \
                                (link.source is None or link.source.name.module != self.module.name):
                continue

            for property_name, property in link.pointers.items():
                if property.target:
                    if isinstance(property.target, str):
                        property.target = localschema.get(property.target, index_only=False)

            if link.source and not isinstance(link.source, proto.Prototype):
                link.source = localschema.get(link.source)

            if link.target and not isinstance(link.target, proto.Prototype):
                link.target = localschema.get(link.target)

            if link.target:
                link.is_atom = isinstance(link.target, proto.Atom)

            cascade_policies = getattr(link, '_cascades', {})

            if cascade_policies:
                self._read_cascade_policies(link, cascade_policies, localschema)

            if link.bases:
                for base in link.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}

                g[link.name]['merge'].extend(b.name for b in link.bases)

        try:
            links = topological.normalize(g, merger=proto.Link.merge, context=localschema)
        except caos.MetaError as e:
            if e.context:
                raise MetaError(e.msg, hint=e.hint, details=e.details, context=e.context.context) from e
            raise

        links = OrderedSet(filter(lambda l: l.name.module == self.module.name, links))

        try:
            for link in links:
                self.normalize_pointer_defaults(link, localschema)

                constraints = getattr(link, '_constraints', ())
                if constraints:
                    self._collect_constraints_for_subject(link, constraints, localschema)

                aconstraints = getattr(link, '_abstract_constraints', ())
                if aconstraints:
                    self._collect_constraints_for_subject(link, aconstraints, localschema,
                                                          abstract=True)

        except caosql_exc.CaosQLReferenceError as e:
            context = lang_context.SourceContext.from_object(index)
            raise MetaError(e.args[0], context=context) from e

        try:
            links = topological.normalize(g, merger=proto.Link.merge_policy, context=localschema)
        except caos.MetaError as e:
            if e.context:
                raise MetaError(e.msg, hint=e.hint, details=e.details, context=e.context.context) from e
            raise

        return OrderedSet(filter(lambda l: l.name.module == self.module.name, links))

    def read_concepts(self, data, localschema):
        for concept_name, concept in data['concepts'].items():
            concept.name = caos.Name(name=concept_name, module=self.module.name)

            self._add_proto(localschema, concept)

        for concept in self.module('concept'):
            bases = []
            custombases = []

            if concept._bases:
                for b in concept._bases:
                    base = self._check_base(concept, b, localschema)

                    if isinstance(base, proto.NativeClassRef):
                        base_cls = get_object(base.class_name)

                        if not issubclass(base_cls, caos.concept.Concept):
                            raise caos.MetaError('custom concept base classes must inherit from '
                                                 'caos.concept.Concept: %s' % base.class_name)
                        custombases.append(base)
                    else:
                        bases.append(base)

            if not bases and concept.name != 'metamagic.caos.builtins.BaseObject':
                bases.append(localschema.get('metamagic.caos.builtins.Object'))

            concept.bases = bases
            concept.custombases = custombases

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

                link.source = concept

                if getattr(link, '_targets', None):
                    targets = []
                    for t in link._targets:
                        target = localschema.get(t)
                        targets.append(target.name)

                    link._targets = targets
                    link._target = self._normalize_link_target_name(link_qname, link._targets,
                                                                    localschema)
                else:
                    link._target = None

                target_name = link._target

                # A new specialized subclass of the link is created for each
                # (source, link_name, target) combination
                link.bases = [localschema.get(link_qname)]

                link_genname = proto.Link.generate_specialized_name(link.source.name, link_qname)
                link.name = caos.Name(name=link_genname, module=self.module.name)

                self.read_properties_for_link(link, localschema)

                self._add_proto(localschema, link)
                concept.add_pointer(link)

        source_pbase = localschema.get('metamagic.caos.builtins.source', type=proto.LinkProperty)
        target_pbase = localschema.get('metamagic.caos.builtins.target', type=proto.LinkProperty)

        for concept in self.module('concept'):
            for link_name, link in concept._links.items():
                link = link.link
                targets = getattr(link, '_targets', ())

                if len(targets) > 1:
                    link.target = self._create_link_target(concept, link, localschema)
                elif targets:
                    link.target = localschema.get(link._target)

                target_pname = caos.Name('metamagic.caos.builtins.target')
                target = proto.LinkProperty(name=target_pname,
                                            bases=[target_pbase],
                                            loading=caos.types.EagerLoading,
                                            _setdefaults_=False, _relaxrequired_=True)
                if link.target:
                    target._target = link.target.name
                else:
                    target._target = caos.Name('metamagic.caos.builtins.none')

                source_pname = caos.Name('metamagic.caos.builtins.source')
                source = proto.LinkProperty(name=source_pname,
                                            bases=[source_pbase],
                                            loading=caos.types.EagerLoading,
                                            _setdefaults_=False, _relaxrequired_=True)
                source._target = link.source.name

                props = {target_pname: target, source_pname: source}
                self._read_properties_for_link(link, props, localschema)

        for concept in self.module('concept'):
            for index in getattr(concept, '_indexes', ()):
                concept.add_index(index)

            constraints = getattr(concept, '_constraints', ())
            if constraints:
                self._collect_constraints_for_subject(concept, constraints, localschema)

            aconstraints = getattr(concept, '_abstract_constraints', ())
            if aconstraints:
                self._collect_constraints_for_subject(concept, aconstraints, localschema,
                                                      abstract=True)


    def _create_link_target(self, source, pointer, localschema):
        targets = [localschema.get(t, type=proto.Concept) for t in pointer._targets]

        target = localschema.get(pointer._target, default=None, type=proto.Concept,
                                 index_only=False)
        if target is None:
            target = pointer.get_common_target(localschema, targets)

            existing = localschema.get(target.name, default=None, type=proto.Concept,
                                       index_only=False)

            if existing is None:
                self._add_proto(localschema, target)
            else:
                target = existing

        return target


    def _normalize_link_target_name(self, link_fqname, targets, localschema):
        if len(targets) == 1:
            return targets[0]
        else:
            return proto.Source.gen_virt_parent_name(targets, module=link_fqname.module)


    def _get_obj_module_aliases(self, obj):
        obj_context = lang_context.SourceContext.from_object(obj)
        module_aliases = {None: str(obj_context.document.import_context)}
        for alias, module in obj_context.document.imports.items():
            module_aliases[alias] = module.__name__
        return module_aliases


    def normalize_index_expr(self, expr, concept, localschema):
        return caosql_utils.normalize_expr(
                    expr, localschema,
                    anchors={'self': concept}, inline_anchors=True)


    def normalize_pointer_defaults(self, source, localschema):
        for ptr in source.own_pointers.values():
            if not ptr.default:
                continue

            defaults = []
            for default in ptr.default:
                if not isinstance(default, caos_types.ExpressionText):
                    defaults.append(default)
                else:
                    def_context = lang_context.SourceContext.from_object(default)

                    if (def_context is None or
                        def_context.document.module.__name__
                            != self.module.name):
                        # This default is from another module, and
                        # so is already normalized.
                        continue

                    module_aliases = {None: str(def_context.document.import_context)}
                    for alias, module in def_context.document.imports.items():
                        module_aliases[alias] = module.__name__

                    ir, _, value = caosql_utils.normalize_tree(
                                        default, localschema,
                                        module_aliases=module_aliases,
                                        anchors={'self': source})

                    first = list(ir.result_types.values())[0][0]

                    if (len(ir.result_types) > 1
                            or not isinstance(first, caos.types.ProtoNode)
                            or (ptr.target is not None
                                    and not first.issubclass(ptr.target))):

                        raise MetaError(('default value query must yield a '
                                         'single-column result of type "%s"') %
                                         ptr.target.name, context=def_context)

                    if ptr.is_pure_computable():
                        # Pure computable without explicit target.
                        # Fixup pointer target and target property.
                        ptr.target = first
                        ptr.is_atom = isinstance(first, proto.Atom)

                        if isinstance(ptr, proto.Link):
                            pname = caos.name.Name('metamagic.caos.builtins.target')
                            tgt_prop = ptr.pointers[pname]
                            tgt_prop.target = first

                    if not isinstance(ptr.target, caos.types.ProtoAtom):
                        if ptr.mapping not in (caos.types.ManyToOne, caos.types.ManyToMany):
                            raise MetaError('concept links with query defaults ' \
                                            'must have either a "*1" or "**" mapping',
                                             context=def_context)

                    defaults.append(caos.types.ExpressionText(value))

            ptr.default = defaults
            ptr.normalize_defaults()


    def order_concepts(self, localschema):
        g = {}

        for concept in self.module('concept'):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}

            for link_name, link in concept.pointers.items():
                if not isinstance(link.source, proto.Prototype):
                    link.source = localschema.get(link.source)

                if not isinstance(link, proto.Computable) and link.source.name == concept.name:
                    if isinstance(link.target, proto.Atom):
                        link.is_atom = True

            if concept.bases:
                for base in concept.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}
                g[concept.name]["merge"].extend(b.name for b in concept.bases)

        concepts = list(filter(lambda c: c.name.module == self.module.name,
                               topological.normalize(g, merger=proto.Concept.merge,
                                                     context=localschema)))

        return concepts


class LinkProps(LangObject):
    def __sx_setstate__(self, data):
        self.props = data['@']


class EntityShellMeta(type(LangObject), type(caos.concept.EntityShell)):
    def __init__(cls, name, bases, dct, *, adapts=None, ignore_aliases=False):
        type(LangObject).__init__(cls, name, bases, dct, adapts=adapts,
                                                         ignore_aliases=ignore_aliases)
        type(caos.concept.EntityShell).__init__(cls, name, bases, dct)


class EntityShell(LangObject, adapts=caos.concept.EntityShell,
                              metaclass=EntityShellMeta):
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


class EntityShell2(LangObject, adapts=caos.concept.EntityShell2,
                               metaclass=EntityShellMeta):

    def _construct(self, concept):
        ent_context = lang_context.SourceContext.from_object(self)
        aliases = {alias: mod.__name__
                   for alias, mod in ent_context.document.imports.items()}
        session = ent_context.document.import_context.session
        cls = session.schema.get(concept, aliases=aliases)
        self.entity = cls(**self.links)

        for (link_name, target), link_properties in self.props.items():
            linkcls = caos.concept.getlink(self.entity, link_name, target)
            linkcls.update(**link_properties)

        imp_ctx = ent_context.document.import_context
        imp_ctx.entities.append(self.entity)
        imp_ctx.unconstructed_entities.discard(self)

    def _process_expr(self, expr):
        ent_context = lang_context.SourceContext.from_object(self)

        aliases = {alias: mod.__name__
                   for alias, mod in ent_context.document.imports.items()}
        session = ent_context.document.import_context.session

        selector = session.selector(expr, module_aliases=aliases)

        if not isinstance(selector, caos.types.ConceptClass):
            raise MetaError('query expressions must return a single entity')

        selector_iter = iter(selector)

        try:
            entity = next(selector_iter)
        except StopIteration:
            msg = 'query returned zero results, expecting exactly one'
            raise MetaError(msg, details=expr, context=ent_context)

        try:
            next(selector_iter)
        except StopIteration:
            pass
        else:
            msg = 'query returned multiple results, expecting exactly one'
            raise MetaError(msg, details=expr, context=ent_context)

        return entity

    def _process_target(self, data):
        if isinstance(data, dict):
            clsname, target = next(iter(data.items()))
            target._construct(clsname)

        elif isinstance(data, ExpressionText):
            target = caos.concept.EntityShell()
            target.entity = self._process_expr(data)

        else:
            target = data

        return target

    def _process_link(self, data):
        if isinstance(data, LinkProps):
            value = self._process_target(data.props.pop('target'))
            target = xvalue(value, **data.props)
        else:
            target = xvalue(self._process_target(data))

        return target

    def __sx_setstate__(self, data):
        caos.concept.EntityShell2.__init__(self)
        ent_ctx = lang_context.SourceContext.from_object(self)
        imp_ctx = ent_ctx.document.import_context
        imp_ctx.unconstructed_entities.add(self)

        links = self.links = {}
        props = self.props = {}

        for link_name, linkval in data.items():
            if not isinstance(linkval, list):
                linkval = [linkval]

            links[link_name] = list()
            for item in linkval:
                lspec = self._process_link(item)

                target = lspec.value
                links[link_name].append(target)
                if lspec.attrs:
                    props[link_name, target] = lspec.attrs


class ProtoSchema(LangObject, adapts=proto.ProtoSchema):
    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        for type in cls.global_dep_order:
            for obj in data(type=type):
                # XXX
                if type in ('link', 'link_property', 'constraint') and not obj.generic():
                    continue
                if type == 'link_property':
                    key = 'link-properties'
                else:
                    key = type + 's'

                try:
                    typdict = result[key]
                except KeyError:
                    typdict = result[key] = {}

                typdict[str(obj.name)] = obj

        return result


class DataSet(LangObject):
    def __sx_setstate__(self, data):
        entities = {id: [shell.entity for shell in shells]
                    for id, shells in data.items() if id}
        context = lang_context.SourceContext.from_object(self)
        session = context.document.import_context.session
        with session.transaction():
            for entity in context.document.import_context.entities:
                entity.__class__.materialize_links(entity, entities)


class DataSet2(LangObject):
    def __sx_setstate__(self, data):
        context = lang_context.SourceContext.from_object(self)
        imp_ctx = context.document.import_context
        session = imp_ctx.session

        with session.transaction():
            for clsname, shell in data:
                shell._construct(clsname)

            if imp_ctx.unconstructed_entities:
                entity = next(iter(imp_ctx.unconstructed_entities))
                e_ctx = lang_context.SourceContext.from_object(entity)
                raise MetaError('invalid link target', context=e_ctx)

            for entity in context.document.import_context.entities:
                entity.__class__.materialize_links(entity, None)


class CaosName(StrLangObject, adapts=caos.Name, ignore_aliases=True):
    def __new__(cls, data):
        return caos.Name.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class ModuleFromData:
    def __init__(self, name):
        self.__name__ = name


class FixtureImportContext(importkit.ImportContext):
    def __new__(cls, name, *, loader=None, session=None, entities=None):
        result = super().__new__(cls, name, loader=loader)
        result.session = session
        result.entities = entities if entities is not None else []
        result.unconstructed_entities = set()
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
        prologue = '%SCHEMA metamagic.caos.backends.yaml.schemas.Semantics\n---\n'
        return prologue + yaml.Language.dump(meta)
