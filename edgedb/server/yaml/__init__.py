##
# Copyright (c) 2008-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import io

import importlib
import builtins
import itertools
import sys
import re

import yaml as pyyaml

import importkit
from importkit import context as lang_context
from importkit import yaml

from metamagic.utils import lang
from metamagic.utils.lang.yaml.struct import MixedStructMeta
from metamagic.utils.nlang import morphology
from metamagic.utils.algos.persistent_hash import persistent_hash
from metamagic.utils.algos import topological
from metamagic.utils.datastructures import xvalue, OrderedSet, typed

from metamagic import caos
from metamagic.caos import schema as lang_protoschema
from metamagic.caos import backends
from metamagic.caos import objects

from metamagic.caos.schema import atoms as s_atoms
from metamagic.caos.schema import attributes as s_attrs
from metamagic.caos.schema import concepts as s_concepts
from metamagic.caos.schema import constraints as s_constr
from metamagic.caos.schema import derivable as s_derivable
from metamagic.caos.schema import enum as s_enum
from metamagic.caos.schema import error as s_err
from metamagic.caos.schema import expr as s_expr
from metamagic.caos.schema import indexes as s_indexes
from metamagic.caos.schema import links as s_links
from metamagic.caos.schema import lproperties as s_lprops
from metamagic.caos.schema import name as sn
from metamagic.caos.schema import objects as s_obj
from metamagic.caos.schema import pointers as s_pointers
from metamagic.caos.schema import policy as s_policy
from metamagic.caos.schema import primary as s_primary
from metamagic.caos.schema import sources as s_sources
from metamagic.caos.schema import schema as s_schema

from metamagic.caos.caosql import errors as caosql_exc
from metamagic.caos.caosql import utils as caosql_utils

from . import delta
from . import protoschema as yaml_protoschema


class MetaError(yaml_protoschema.SchemaError, s_err.SchemaError):
    pass


class SimpleLangObject(yaml.Object):
    pass


class LangObjectMeta(type(yaml.Object), type(s_obj.BasePrototype)):
    def __init__(cls, name, bases, dct, *, adapts=None, ignore_aliases=False):
        type(yaml.Object).__init__(cls, name, bases, dct, adapts=adapts,
                                                          ignore_aliases=ignore_aliases)
        type(s_obj.BasePrototype).__init__(cls, name, bases, dct)


class LangObject(yaml.Object, metaclass=LangObjectMeta):
    @classmethod
    def get_canonical_class(cls):
        for base in cls.__bases__:
            if issubclass(base, s_obj.ProtoObject) and not issubclass(base, LangObject):
                return base

        return cls


class ImportContext(yaml.Object, adapts=lang_protoschema.ImportContext):
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


class StrLangObject(SimpleLangObject):
    def __reduce__(self):
        return (self.__class__.get_adaptee(), (str(self),))

    def __getstate__(self):
        return {}

    @classmethod
    def __sx_getnewargs__(cls, context, data):
        return (data,)


class StrEnumMeta(type(StrLangObject), type(s_enum.StrEnum)):
    @staticmethod
    def _get_mixins_(bases):
        return str, bases[1]

    @classmethod
    def __prepare__(metacls, cls, bases, *, adapts=None, ignore_aliases=False):
        return type(s_enum.StrEnum).__prepare__(cls, bases)

    def __new__(mcls, name, bases, dct, *, adapts=None, ignore_aliases=False):
        result = type(StrLangObject).__new__(
            mcls, name, bases, dct, adapts=adapts,
            ignore_aliases=ignore_aliases)

        if adapts is not None:
            result._member_map_ = adapts._member_map_
            result._member_names_ = adapts._member_names_
            result._member_type_ = adapts._member_type_
            result._value2member_map_ = adapts._value2member_map_

        return result

    def __init__(cls, name, bases, dct, *, adapts=None, ignore_aliases=False):
        type(StrLangObject).__init__(
            cls, name, bases, dct, adapts=adapts,
            ignore_aliases=ignore_aliases)


class ExpressionText(StrLangObject, adapts=s_expr.ExpressionText,
                                    ignore_aliases=True):
    @classmethod
    def __sx_getstate__(cls, data):
        return pyyaml.ScalarNode('!expr', str(data), style='|')


class LinkMapping(StrLangObject, adapts=s_links.LinkMapping,
                  ignore_aliases=True, metaclass=StrEnumMeta):
    def __new__(cls, data):
        return s_links.LinkMapping.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class PointerExposedBehaviour(StrLangObject,
                              adapts=s_pointers.PointerExposedBehaviour,
                              ignore_aliases=True, metaclass=StrEnumMeta):
    def __new__(cls, data):
        return s_pointers.PointerExposedBehaviour.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class PointerLoading(StrLangObject, adapts=s_pointers.PointerLoading,
                     ignore_aliases=True, metaclass=StrEnumMeta):
    def __new__(cls, data):
        return s_pointers.PointerLoading.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class LinkSearchWeight(StrLangObject, adapts=s_links.LinkSearchWeight,
                       ignore_aliases=True, metaclass=StrEnumMeta):
    def __new__(cls, data):
        return s_links.LinkSearchWeight.__new__(cls, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data)


class TypedCollectionMeta(type(yaml.Object),
                          type(typed.AbstractTypedCollection)):
    def __new__(mcls, name, bases, dct, *,
                      adapts=None, ignore_aliases=False, type=None):
        builtins.type(typed.TypedSet).__new__(
            mcls, name, bases, dct, type=type)
        result = builtins.type(yaml.Object).__new__(
            mcls, name, bases, dct, adapts=adapts,
            ignore_aliases=ignore_aliases, type=type)

        return result

    def __init__(cls, name, bases, dct, *,
                      adapts=None, ignore_aliases=False, type=None):
        builtins.type(typed.TypedSet).__init__(
            cls, name, bases, dct, type=type)
        builtins.type(yaml.Object).__init__(
            cls, name, bases, dct, adapts=adapts, ignore_aliases=ignore_aliases)


class AbstractTypedCollection(yaml.Object, metaclass=TypedCollectionMeta,
                              adapts=typed.AbstractTypedCollection, type=object):
    _TYPE_ARGS = ('type',)

    def __sx_setstate__(self, data):
        raise NotImplementedError

    @classmethod
    def __sx_getstate__(cls, data):
        raise NotImplementedError


class TypedDict(AbstractTypedCollection, adapts=typed.TypedDict, type=object):
    def __sx_setstate__(self, data):
        cls = self.__class__.get_adaptee()
        cls.__init__(self, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return dict(data.items())


class TypedList(AbstractTypedCollection, adapts=typed.TypedList, type=object):
    def __sx_setstate__(self, data):
        cls = self.__class__.get_adaptee()
        cls.__init__(self, data)

    @classmethod
    def __sx_getstate__(cls, data):
        return list(data)


class PrototypeDict(TypedDict, adapts=s_obj.PrototypeDict, type=object):
    pass


class ArgDict(TypedDict, adapts=s_obj.ArgDict, type=object):
    pass


class SchemaTypeConstraintSet(AbstractTypedCollection,
                              adapts=s_obj.SchemaTypeConstraintSet,
                              type=s_obj.SchemaTypeConstraint):
    def __sx_setstate__(self, data):
        if not isinstance(data, list):
            data = (data,)

        constrs = set()

        for constr_type, constr_data in data:
            if constr_type == 'enum':
                constr = s_obj.SchemaTypeConstraintEnum(data=constr_data)
            else:
                context = lang_context.SourceContext.from_object(self)
                msg = 'invalid schema type constraint type: {!r}'.format(constr_type)
                raise MetaError(msg, context=context)

            constrs.add(constr)

        s_obj.SchemaTypeConstraintSet.__init__(self, constrs)

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


class SchemaTypeConstraintEnum(LangObject,
                               adapts=s_obj.SchemaTypeConstraintEnum):
    _name = 'enum'

    @classmethod
    def __sx_getstate__(cls, data):
        return [cls._name, list(data.data)]


class SchemaType(LangObject, adapts=s_obj.SchemaType, ignore_aliases=True):
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

        s_obj.SchemaType.__init__(self, main_type=main_type,
                                  element_type=element_type,
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


class DerivablePrototypeMeta(type(LangObject),
                             type(s_derivable.DerivablePrototype)):
    pass


class DerivablePrototype(LangObject, adapts=s_derivable.DerivablePrototype,
                         metaclass=DerivablePrototypeMeta):
    pass


class PrototypeMeta(type(LangObject), type(s_primary.Prototype),
                    MixedStructMeta):
    pass


class Prototype(LangObject, adapts=s_primary.Prototype,
                metaclass=PrototypeMeta):
    pass


class DefaultSpecList(AbstractTypedCollection, adapts=s_expr.ExpressionList,
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

        s_expr.ExpressionList.__init__(self, default)

    @classmethod
    def __sx_getstate__(cls, data):
        return list(data)


class Attribute(Prototype, adapts=s_attrs.Attribute):
    def __sx_setstate__(self, data):
        s_attrs.Attribute.__init__(self, name=None, title=data['title'],
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


class AttributeValue(DerivablePrototype, ignore_aliases=True,
                     adapts=s_attrs.AttributeValue):
    def __sx_setstate__(self, data):
        s_attrs.AttributeValue.__init__(self, value=data, _setdefaults_=False, _relaxrequired_=True)

    @classmethod
    def __sx_getstate__(cls, data):
        return data.value


class Constraint(Prototype, adapts=s_constr.Constraint):
    def __sx_setstate__(self, data):
        if isinstance(data, dict):
            extends = data.get('extends')
            if extends:
                if not isinstance(extends, list):
                    extends = [extends]

            s_constr.Constraint.__init__(self, title=data['title'], description=data['description'],
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
            s_constr.Constraint.__init__(self, _setdefaults_=False, _relaxrequired_=True)
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


default_name = None

class Atom(Prototype, adapts=s_atoms.Atom):
    def __sx_setstate__(self, data):
        s_atoms.Atom.__init__(self, name=default_name,
                            default=data['default'], title=data['title'],
                            description=data['description'],
                            is_abstract=data['abstract'],
                            is_final=data['final'],
                            _setdefaults_=False, _relaxrequired_=True)
        self._attributes = data.get('attributes')
        self._constraints = data.get('constraints')
        self._bases = [data['extends']] if data['extends'] else []
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


class Concept(Prototype, adapts=s_concepts.Concept):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        s_concepts.Concept.__init__(self, name=default_name,
                               title=data.get('title'), description=data.get('description'),
                               is_abstract=data.get('abstract'), is_final=data.get('final'),
                               _setdefaults_=False, _relaxrequired_=True)
        self._bases = extends
        self._links = data.get('links', {})
        self._indexes = list((data.get('indexes') or {}).items())
        self._constraints = data.get('constraints')
        self._abstract_constraints = data.get('abstract-constraints')
        self._yml_workattrs = {'_links', '_indexes', '_bases', '_constraints',
                               '_abstract_constraints'}

    @classmethod
    def __sx_getstate__(cls, data):
        result = {
            'extends': [b.name for b in data.bases]
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
                if ptr.target is None or (isinstance(ptr.target, s_concepts.Concept) and ptr.target.is_virtual):
                    # key = tuple(t.name for t in ptr.target._virtual_children)
                    key = 'virtual___'
                else:
                    key = ptr.target.name

                result['links'][ptr_name] = {key: ptr}

        if data.indexes:
            result['indexes'] = dict(data.local_indexes)

        if data.local_constraints:
            result['constraints'] = dict(data.local_constraints)

        return result


class SourceIndex(DerivablePrototype, adapts=s_indexes.SourceIndex,
                  ignore_aliases=True):
    def __sx_setstate__(self, data):
        s_indexes.SourceIndex.__init__(self, expr=data, _setdefaults_=False,
                                                    _relaxrequired_=True)

    @classmethod
    def __sx_getstate__(cls, data):
        return str(data.expr)


class Action(Prototype, adapts=s_policy.Action):
    def __sx_setstate__(self, data):
        s_policy.Action.__init__(self, name=default_name, title=data['title'],
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


class ActionSet(AbstractTypedCollection, adapts=s_policy.ActionSet,
                                         type=s_policy.Action):
    def __sx_setstate__(self, data):
        if not isinstance(data, list):
            data = (data,)

        items = []
        for name in data:
            item = s_policy.Action(_setdefaults_=False, _relaxrequired_=True)
            item._name = name
            items.append(item)

        s_policy.ActionSet.__init__(self, items)

    @classmethod
    def __sx_getstate__(cls, data):
        # prototype_name here is for PrototypeRefs, which will
        # appear in partial schema loads, such as with delta loads.
        return [getattr(item, 'prototype_name', item.name) for item in data]


class Event(Prototype, adapts=s_policy.Event):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        s_policy.Event.__init__(self, name=default_name, title=data['title'],
                                   description=data['description'],
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

        return result


class Policy(DerivablePrototype, adapts=s_policy.Policy):
    @classmethod
    def __sx_getstate__(cls, data):
        result = {}

        result['name'] = data.name
        result['subject'] = data.subject.name
        result['event'] = data.event.name
        result['actions'] = [a.name for a in data.actions]

        return result


class LinkPropertyDef(Prototype, s_lprops.LinkProperty):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        s_lprops.LinkProperty.__init__(self, name=default_name, title=data['title'],
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


class LinkProperty(Prototype, adapts=s_lprops.LinkProperty, ignore_aliases=True):
    def __sx_setstate__(self, data):
        if isinstance(data, ExpressionText):
            context = lang_context.SourceContext.from_object(self)
            lang_context.SourceContext.register_object(data, context)
            s_lprops.LinkProperty.__init__(self, _setdefaults_=False,
                                              _relaxrequired_=True,
                                              default=[data],
                                              readonly=True)

        elif isinstance(data, str):
            s_lprops.LinkProperty.__init__(self, name=default_name,
                                              _setdefaults_=False,
                                              _relaxrequired_=True)
            self._target = data
            self._yml_workattrs = {'_target'}

        else:
            atom_name, info = next(iter(data.items()))

            s_lprops.LinkProperty.__init__(self, name=default_name,
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


class LinkPropertyProps(Prototype, s_lprops.LinkProperty):
    def __sx_setstate__(self, data):
        s_lprops.LinkProperty.__init__(self, name=default_name,
                                    default=data['default'],
                                    _setdefaults_=False,
                                    _relaxrequired_=True)


class LinkDef(Prototype, adapts=s_links.Link):
    def __sx_setstate__(self, data):
        extends = data.get('extends')
        if extends:
            if not isinstance(extends, list):
                extends = [extends]

        s_links.Link.__init__(self, name=default_name,
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
        self._indexes = list((data.get('indexes') or {}).items())
        self._policy = data.get('policy')
        self._yml_workattrs = {'_properties', '_indexes', '_policy', '_bases'}

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

        if data.local_indexes:
            result['indexes'] = dict(data.local_indexes)

        if data.local_policy:
            result['policy'] = dict(data.local_policy)

        return result


class LinkSearchConfiguration(LangObject, adapts=s_links.LinkSearchConfiguration, ignore_aliases=True):
    def __sx_setstate__(self, data):
        if isinstance(data, bool):
            if data:
                weight = s_links.LinkSearchWeight.A
            else:
                weight = None
        else:
            if data:
                weight = s_links.LinkSearchWeight(data['weight'])
            else:
                weight = None

        s_links.LinkSearchConfiguration.__init__(self, weight=weight)

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
            link = s_links.Link(_setdefaults_=False, _relaxrequired_=True,
                              default=[data], readonly=True)
            lang_context.SourceContext.register_object(link, context)
            self.link = link

        elif isinstance(data, (str, list)):
            link = s_links.Link(source=None, target=None, name=default_name, _setdefaults_=False,
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

            link = s_links.Link(name=default_name, target=None,
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
            link._abstract_constraints = info.get('abstract-constraints')
            link._properties = props
            link._targets = targets
            link._policy = info.get('policy')

            link._yml_workattrs = {'_constraints', '_abstract_constraints',
                                   '_properties', '_targets', '_policy'}

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
            except s_err.SchemaError:
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

        self.read_actions(data, localschema)
        self.read_events(data, localschema)
        self.read_link_properties(data, localschema)
        self.read_links(data, localschema)
        self.read_concepts(data, localschema)
        self.read_policy(data, localschema)

        # The final pass on may produce additional objects,
        # thus, it has to be performed in reverse order (mostly).
        concepts = OrderedSet(self.order_concepts(localschema))
        links = OrderedSet(self.order_links(localschema))
        linkprops = OrderedSet(self.order_link_properties(localschema))
        policy = OrderedSet(self.order_policy(localschema))
        events = OrderedSet(self.order_events(localschema))
        actions = OrderedSet(self.order_actions(localschema))
        attribute_values = OrderedSet(self.order_attribute_values(localschema))
        indexes = OrderedSet(self.order_indexes(localschema))

        constraints.update(self.collect_derived_constraints(localschema))
        self.finalize_constraints(constraints, localschema)

        for attribute in attributes:
            attribute.setdefaults()

        for attribute_value in attribute_values:
            attribute_value.setdefaults()

        for action in actions:
            action.setdefaults()

        for event in events:
            event.setdefaults()

        for policy_item in policy:
            policy_item.setdefaults()

        for atom in atoms:
            atom.setdefaults()
            if hasattr(atom, '_yml_workattrs'):
                for workattr in atom._yml_workattrs:
                    delattr(atom, workattr)
                delattr(atom, '_yml_workattrs')

        for atom in atoms:
            atom.finalize(localschema)

        for index in indexes:
            index.setdefaults()

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
            for index in concept.local_indexes.values():
                expr = self.normalize_index_expr(index.expr, concept, localschema)
                index.expr = expr

        # Link meterialization might have produced new atoms
        for atom in localschema(type=s_atoms.Atom):
            if atom.name.module == self.module.name:
                atoms.add(atom)

        for constraint in localschema(type=s_constr.Constraint):
            if constraint.name.module == self.module.name:
                constraints.add(constraint)

        # Link meterialization might have produced new specialized properties
        for prop in localschema(type=s_lprops.LinkProperty):
            if prop.name.module != self.module.name:
                self._add_foreign_proto(prop)
            else:
                linkprops.add(prop)

        # Concept meterialization might have produced new specialized links
        for link in localschema(type=s_links.Link):
            if link.name.module != self.module.name:
                self._add_foreign_proto(link)
            else:
                links.add(link)

        # Arrange prototypes in the resulting schema according to determined topological order.
        localschema.reorder(itertools.chain(
            attributes, attribute_values, actions, events, constraints,
            atoms, linkprops, indexes, links, concepts, policy))


    def _check_base(self, element, base_name, localschema):
        base = localschema.get(base_name,
                               type=element.__class__.get_canonical_class(),
                               index_only=False)
        if base.is_final:
            context = lang_context.SourceContext.from_object(element)
            msg = '{!r} is final and cannot be inherited from'.format(base.name)
            raise MetaError(msg, context=context)

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

        objs = topological.normalize(g, merger=objmerger, schema=localschema)
        return OrderedSet(filter(lambda obj: obj.name.module == self.module.name, objs))


    def _parse_typeref(self, typeref, localschema, context):
        try:
            collection_type, type = s_obj.TypeRef.parse(typeref)
        except ValueError as e:
            raise MetaError(e.args[0], context=context) from None

        if type is not None:
            try:
                type = localschema.get(type)
            except s_err.SchemaError as e:
                raise MetaError(e, context=context)

        if collection_type is not None:
            type = collection_type(element_type=type)

        return type

    def collect_attributes(self, data, localschema):
        for attribute_name, attribute in data['attributes'].items():
            attribute.name = sn.Name(name=attribute_name, module=self.module.name)
            self._add_proto(localschema, attribute)


    def merge_and_sort_attributes(self, localschema):
        return OrderedSet(self.module('attribute'))


    def order_attribute_values(self, localschema):
        return self.module('attribute-value')


    def order_indexes(self, localschema):
        return self.module('index')


    def read_actions(self, data, localschema):
        for action_name, action in data['actions'].items():
            action.name = sn.Name(name=action_name, module=self.module.name)
            self._add_proto(localschema, action)


    def order_actions(self, localschema):
        return self.module('action')


    def read_events(self, data, localschema):
        for event_name, event in data['events'].items():
            event.name = sn.Name(name=event_name, module=self.module.name)
            self._add_proto(localschema, event)

        for event in self.module('event'):
            if event._bases:
                try:
                    event.bases = [self._check_base(event, b, localschema)
                                   for b in event._bases]
                except s_err.SchemaError as e:
                    context = lang_context.SourceContext.from_object(event)
                    raise MetaError(e, context=context) from e

            elif event.name != 'metamagic.caos.builtins.event':
                # All event inherit from builtins.event
                event.bases = [localschema.get('metamagic.caos.builtins.event')]
            else:
                event.bases = []


    def order_events(self, localschema):
        g = {}

        for event in self.module('event'):
            g[event.name] = {"item": event, "merge": [], "deps": []}

            if event.bases:
                for base in event.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}

                g[event.name]['merge'].extend(b.name for b in event.bases)

        atoms = topological.normalize(g, merger=s_policy.Event.merge,
                                         schema=localschema)
        return list(filter(lambda a: a.name.module == self.module.name, atoms))


    def _normalize_attribute_values(self, localschema, subject, attributes):
        attrs = {}

        for attribute, attrvalue in attributes.items():
            attribute = localschema.get(attribute, index_only=False)

            name = s_attrs.AttributeValue.generate_specialized_name(subject.name,
                                                                            attribute.name)

            name = sn.Name(name=name, module=self.module.name)

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


    def _read_policies(self, pointer, policies, localschema):
        for event, action in policies.items():
            event = localschema.get(event, index_only=False)
            action = localschema.get(action, index_only=False)

            name = s_policy.Policy.generate_specialized_name(
                                    pointer.name, event.name)
            name = sn.Name(name=name, module=self.module.name)

            policy = s_policy.Policy(name=name, subject=pointer, event=event,
                                  actions=[action])

            self._add_proto(localschema, policy)
            pointer.add_policy(policy)


    def read_policy(self, data, localschema):
        data = data.get('policies')

        links = data.get('links') if data else None
        if links:
            for link_name, link_data in data['links'].items():
                link = localschema.get(link_name)
                policies = link_data.get('policy')
                if policies:
                    self._read_policies(link, policies, localschema)

        concepts = data.get('concepts') if data else None
        if concepts:
            for concept_name, concept_data in concepts.items():
                concept = localschema.get(concept_name)

                links = concept_data.get('links')

                if links:
                    for link_name, link_data in links.items():
                        if not sn.Name.is_qualified(link_name):
                            # If the name is not fully qualified, assume inline link definition.
                            # The only attribute that is used for global definition is the name.
                            link_qname = sn.Name(name=link_name, module=self.module.name)
                        else:
                            link_qname = sn.Name(link_name)

                        genlink = localschema.get(link_qname)

                        speclink = concept.pointers[genlink.name]

                        policies = link_data.get('policy')
                        if policies:
                            self._read_policies(speclink, policies, localschema)


    def order_policy(self, localschema):
        return self.module('policy')


    def collect_atoms(self, data, localschema):
        # First pass on atoms.
        #
        # Keeps this very simple and do not attempt to resolve anything
        # besides bases to avoid circular dependency, as atoms are used
        # in attribute and constraint definitions.
        #
        for atom_name, atom in data['atoms'].items():
            atom.name = sn.Name(name=atom_name, module=self.module.name)
            self._add_proto(localschema, atom)

        for atom in self.module('atom'):
            if atom._bases:
                try:
                    atom.bases = [self._check_base(atom, atom._bases[0], localschema)]
                except s_err.SchemaError as e:
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

                    ptypes = {p.element_type if isinstance(p, s_obj.Collection) else p
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

                    if isinstance(atom_base, s_atoms.Atom) and atom.name:
                        this_item['merge'].append(atom_base.name)
                        if atom_base.name.module != self.module.name:
                            g[atom_base.name] = {"item": atom_base, "merge": [], "deps": []}

        atoms = topological.normalize(g, merger=s_atoms.Atom.merge,
                                         schema=localschema)
        return OrderedSet(filter(lambda a: a.name.module == self.module.name, atoms))

    def collect_constraints(self, data, localschema):
        # First pass on constraint definitions.
        #
        # Constraints potentially depend on atoms, so this must be called after
        # collect_atoms().
        #

        for constraint_name, constraint in data['constraints'].items():
            constraint.name = sn.Name(name=constraint_name, module=self.module.name)
            self._add_proto(localschema, constraint)

        for constraint in self.module('constraint'):
            context = lang_context.SourceContext.from_object(constraint)
            module_aliases = self._get_obj_module_aliases(constraint)

            if constraint._bases:
                try:
                    constraint.bases = [self._check_base(constraint, b, localschema)
                                        for b in constraint._bases]
                except s_err.SchemaError as e:
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
                    expr = s_constr.Constraint.normalize_constraint_expr(
                        localschema, module_aliases, constraint._expr)
                except (ValueError, caosql_exc.CaosQLQueryError) as e:
                    raise MetaError(e.args[0], context=context) from None

                constraint.expr = expr

            if constraint._subjectexpr:
                # Again, no interpretation, simple validation
                try:
                    expr = s_constr.Constraint.normalize_constraint_subject_expr(
                        localschema, module_aliases, constraint._subjectexpr)
                except (ValueError, caosql_exc.CaosQLQueryError) as e:
                    raise MetaError(e.args[0], context=context) from None

                constraint.subjectexpr = expr


    def merge_and_sort_constraints(self, localschema):
        return self._merge_and_sort_objects(
            localschema, 'constraint', s_constr.Constraint.merge)


    def collect_derived_constraints(self, localschema):
        constraints = self.module('constraint')
        return OrderedSet(c for c in constraints if c.subject is not None)


    def finalize_constraints(self, constraints, localschema):
        for constraint in constraints:
            constraint.acquire_ancestor_inheritance(localschema)
            constraint.setdefaults()

        return constraints


    def _collect_constraints_for_subject(self, subject, constraints,
                                               localschema, abstract=False):
        # Perform initial collection of constraints defined in subject context.
        # At this point all referenced constraints should be fully initialized.

        namegen = s_constr.Constraint.generate_specialized_name

        constr = {}

        for constraint_name, constraint in constraints:
            if constraint._expr:
                constraint.expr = constraint._expr

            constraint_base = localschema.get(constraint_name,
                                              type=s_constr.Constraint)
            constraint_qname = constraint_base.name

            # A new specialized subclass of the constraint is created
            # for each subject referencing the constraint.
            #
            constraint.bases = [localschema.get(constraint_qname,
                                                type=s_constr.Constraint)]
            constraint.subject = subject
            constraint.acquire_ancestor_inheritance(localschema)

            constr_genname = namegen(subject.name, constraint.bases[0].name)
            constraint.name = sn.Name(name=constr_genname,
                                        module=self.module.name)
            constraint.is_abstract = abstract

            # We now have a full set of data to perform final validation
            # and analysis of the constraint.
            #
            s_constr.Constraint.process_specialized_constraint(
                localschema, constraint)

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
                constraint.merge(prev, schema=localschema)
                constraint.merge_localexprs(prev, schema=localschema)
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
            property.name = sn.Name(name=property_name, module=module)

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
                                              type=s_lprops.LinkProperty)]


    def order_link_properties(self, localschema):
        g = {}

        for prop in self.module('link_property'):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}

            if prop.bases:
                g[prop.name]['merge'].extend(pb.name for pb in prop.bases)

                for base in prop.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}

        p = topological.normalize(g, merger=s_lprops.LinkProperty.merge,
                                     schema=localschema)
        return list(filter(lambda p: p.name.module == self.module.name, p))


    def read_properties_for_link(self, link, localschema):
        props = getattr(link, '_properties', None)
        if not props:
            return

        return self._read_properties_for_link(link, props, localschema)

    def _read_properties_for_link(self, link, props, localschema):
        for property_name, property in props.items():

            property_base = localschema.get(property_name, type=s_lprops.LinkProperty, default=None,
                                            index_only=False)

            if property_base is None:
                if not link.generic():
                    # Only generic links can implicitly define properties
                    raise s_err.SchemaError('reference to an undefined property "%s"' % property_name)

                # The link property has not been defined globally.
                if not sn.Name.is_qualified(property_name):
                    # If the name is not fully qualified, assume inline link property
                    # definition. The only attribute that is used for global definition
                    # is the name.
                    property_qname = sn.Name(name=property_name, module=self.module.name)
                    propdef_base = localschema.get('metamagic.caos.builtins.link_property',
                                                   type=s_lprops.LinkProperty)
                    propdef = s_lprops.LinkProperty(name=property_qname, bases=[propdef_base])
                    self._add_proto(localschema, propdef)
                else:
                    property_qname = sn.Name(property_name)
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
                    raise s_err.SchemaError('link "%s" does not define property "%s"' \
                                         % (link.name, property_qname))
                property_qname = propdef.normal_name()

            # A new specialized subclass of the link property is created for each
            # (source, property_name, target_atom) combination
            property.bases = [localschema.get(property_qname, type=s_lprops.LinkProperty)]
            prop_genname = s_lprops.LinkProperty.generate_specialized_name(link.name, property_qname)

            property.name = sn.Name(name=prop_genname, module=self.module.name)
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

    def _collect_indexes_for_subject(self, subject, indexes, localschema):
        for index_name, index in indexes:
            index_name = subject.name + '.' + index_name
            local_name = index.__class__.generate_specialized_name(
                                            subject.name, index_name)
            index.name = sn.Name(name=local_name,
                                        module=self.module.name)
            index.expr = self.normalize_index_expr(index.expr, subject,
                                                   localschema)
            index.subject = subject
            subject.add_index(index)
            self._add_proto(localschema, index)

    def _create_base_link(self, link, link_qname, localschema, type=None):
        type = type or s_links.Link

        base = 'metamagic.caos.builtins.link' if type is s_links.Link else \
               'metamagic.caos.builtins.link_property'

        base = localschema.get(base, type=type)
        linkdef = type(name=link_qname, bases=[base], _setdefaults_=False)

        self._add_proto(localschema, linkdef)
        return linkdef

    def read_links(self, data, localschema):
        for link_name, link in data['links'].items():
            module = self.module.name
            link.name = sn.Name(name=link_name, module=module)
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

            if link.source and not isinstance(link.source, s_obj.BasePrototype):
                link.source = localschema.get(link.source)

            if link.target and not isinstance(link.target, s_obj.BasePrototype):
                link.target = localschema.get(link.target)

            policies = getattr(link, '_policy', {})

            if policies:
                self._read_policies(link, policies, localschema)

            if link.bases:
                for base in link.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}

                g[link.name]['merge'].extend(b.name for b in link.bases)

        try:
            links = topological.normalize(g, merger=s_links.Link.merge,
                                             schema=localschema)
        except s_err.SchemaError as e:
            if e.context:
                raise MetaError(e.msg, hint=e.hint, details=e.details, context=e.context.context) from e
            raise

        links = OrderedSet(filter(lambda l: l.name.module == self.module.name, links))

        try:
            for link in links:
                self.normalize_pointer_defaults(link, localschema)

                indexes = getattr(link, '_indexes', None)
                if indexes:
                    self._collect_indexes_for_subject(
                            link, indexes, localschema)

                constraints = getattr(link, '_constraints', ())
                if constraints:
                    self._collect_constraints_for_subject(
                            link, constraints, localschema)

                aconstraints = getattr(link, '_abstract_constraints', ())
                if aconstraints:
                    self._collect_constraints_for_subject(
                            link, aconstraints, localschema, abstract=True)

        except caosql_exc.CaosQLReferenceError as e:
            context = lang_context.SourceContext.from_object(link)
            raise MetaError(e.args[0], context=context) from e

        return OrderedSet(filter(lambda l: l.name.module == self.module.name, links))

    def read_concepts(self, data, localschema):
        for concept_name, concept in data['concepts'].items():
            concept.name = sn.Name(name=concept_name, module=self.module.name)

            self._add_proto(localschema, concept)

        for concept in self.module('concept'):
            bases = []

            if concept._bases:
                for b in concept._bases:
                    base = self._check_base(concept, b, localschema)
                    bases.append(base)

            if not bases and concept.name != 'metamagic.caos.builtins.BaseObject':
                bases.append(localschema.get('metamagic.caos.builtins.Object'))

            concept.bases = bases

            for link_name, link in concept._links.items():
                link = link.link
                link_base = localschema.get(link_name, type=s_links.Link, default=None,
                                            index_only=False)
                if link_base is None:
                    # The link has not been defined globally.
                    if not sn.Name.is_qualified(link_name):
                        # If the name is not fully qualified, assume inline link definition.
                        # The only attribute that is used for global definition is the name.
                        link_qname = sn.Name(name=link_name, module=self.module.name)
                        self._create_base_link(link, link_qname, localschema)
                    else:
                        link_qname = sn.Name(link_name)
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

                link_genname = s_links.Link.generate_specialized_name(link.source.name, link_qname)
                link.name = sn.Name(name=link_genname, module=self.module.name)

                self.read_properties_for_link(link, localschema)

                self._add_proto(localschema, link)
                concept.add_pointer(link)

        source_pbase = localschema.get('metamagic.caos.builtins.source', type=s_lprops.LinkProperty)
        target_pbase = localschema.get('metamagic.caos.builtins.target', type=s_lprops.LinkProperty)

        for concept in self.module('concept'):
            for link_name, link in concept._links.items():
                link = link.link
                targets = getattr(link, '_targets', ())

                if len(targets) > 1:
                    link.spectargets = s_obj.PrototypeSet(
                        localschema.get(t) for t in targets
                    )

                    link.target = self._create_link_target(
                                    concept, link, localschema)
                elif targets:
                    link.target = localschema.get(link._target)

                target_pname = sn.Name('metamagic.caos.builtins.target')
                target = s_lprops.LinkProperty(name=target_pname,
                                            bases=[target_pbase],
                                            loading=s_pointers.PointerLoading.Eager,
                                            readonly=True,
                                            _setdefaults_=False, _relaxrequired_=True)
                if link.target:
                    target._target = link.target.name
                else:
                    target._target = sn.Name('metamagic.caos.builtins.none')

                source_pname = sn.Name('metamagic.caos.builtins.source')
                source = s_lprops.LinkProperty(name=source_pname,
                                            bases=[source_pbase],
                                            loading=s_pointers.PointerLoading.Eager,
                                            readonly=True,
                                            required=True,
                                            _setdefaults_=False, _relaxrequired_=True)
                source._target = link.source.name

                props = {target_pname: target, source_pname: source}
                self._read_properties_for_link(link, props, localschema)

        for concept in self.module('concept'):
            indexes = getattr(concept, '_indexes', ())
            if indexes:
                self._collect_indexes_for_subject(concept, indexes,
                                                  localschema)

            constraints = getattr(concept, '_constraints', ())
            if constraints:
                self._collect_constraints_for_subject(concept, constraints,
                                                      localschema)

            aconstraints = getattr(concept, '_abstract_constraints', ())
            if aconstraints:
                self._collect_constraints_for_subject(concept, aconstraints,
                                                      localschema,
                                                      abstract=True)


    def _create_link_target(self, source, pointer, localschema):
        targets = [localschema.get(t, type=s_concepts.Concept)
                   for t in pointer._targets]

        target = localschema.get(pointer._target, default=None,
                                 type=s_concepts.Concept, index_only=False)
        if target is None:
            target = pointer.get_common_target(localschema, targets)
            target.is_derived = True

            existing = localschema.get(target.name, default=None,
                                       type=s_concepts.Concept, index_only=False)

            if existing is None:
                self._add_proto(localschema, target)
            else:
                target = existing

        return target


    def _normalize_link_target_name(self, link_fqname, targets, localschema):
        if len(targets) == 1:
            return targets[0]
        else:
            return s_sources.Source.gen_virt_parent_name(targets, module=link_fqname.module)


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
                if not isinstance(default, s_expr.ExpressionText):
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
                            or not isinstance(first, s_obj.ProtoNode)
                            or (ptr.target is not None
                                    and not first.issubclass(ptr.target))):

                        raise MetaError(('default value query must yield a '
                                         'single-column result of type "%s"') %
                                         ptr.target.name, context=def_context)

                    if ptr.is_pure_computable():
                        # Pure computable without explicit target.
                        # Fixup pointer target and target property.
                        ptr.target = first

                        if isinstance(ptr, s_links.Link):
                            pname = sn.Name('metamagic.caos.builtins.target')
                            tgt_prop = ptr.pointers[pname]
                            tgt_prop.target = first

                    if not isinstance(ptr.target, s_atoms.Atom):
                        if ptr.mapping not in (s_links.LinkMapping.ManyToOne, s_links.LinkMapping.ManyToMany):
                            raise MetaError('concept links with query defaults ' \
                                            'must have either a "*1" or "**" mapping',
                                             context=def_context)

                    defaults.append(s_expr.ExpressionText(value))

            ptr.default = defaults
            ptr.normalize_defaults()


    def order_concepts(self, localschema):
        g = {}

        for concept in self.module('concept'):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}

            for link_name, link in concept.pointers.items():
                if not isinstance(link.source, s_obj.BasePrototype):
                    link.source = localschema.get(link.source)

            if concept.bases:
                for base in concept.bases:
                    if base.name.module != self.module.name:
                        g[base.name] = {"item": base, "merge": [], "deps": []}
                g[concept.name]["merge"].extend(b.name for b in concept.bases)

        ordered = topological.normalize(g, merger=s_concepts.Concept.merge,
                                           schema=localschema)
        concepts = list(filter(lambda c: c.name.module == self.module.name,
                               ordered))

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
            proto = getattr(selector, '__sx_prototype__', None)

            if not isinstance(proto, s_concepts.Concept):
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
        proto = getattr(selector, '__sx_prototype__', None)

        if not isinstance(proto, s_concepts.Concept):
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


class ProtoSchema(LangObject, adapts=lang_protoschema.ProtoSchema):
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


class CaosName(StrLangObject, adapts=sn.Name, ignore_aliases=True):
    def __new__(cls, data):
        return sn.Name.__new__(cls, data)

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
        import_context = s_schema.ImportContext('<string>')
        module = ModuleFromData('<string>')
        context = lang_context.DocumentContext(module=module, import_context=import_context)
        for k, v in lang.yaml.Language.load_dict(io.StringIO(data), context):
            setattr(module, str(k), v)

        return module

    def getschema(self):
        return self._schema

    def dump_schema(self, schema):
        prologue = '%SCHEMA metamagic.caos.backends.yaml.schemas.Semantics\n---\n'
        return prologue + yaml.Language.dump(schema)
