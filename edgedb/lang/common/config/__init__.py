##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import functools
import itertools
import inspect

from semantix.utils.functional import decorate, apply_decorator, get_argsspec
from semantix.utils.functional.types import Checker, FunctionValidator, checktypes, \
                                            ChecktypeExempt, TypeChecker, CombinedChecker
from semantix.exceptions import SemantixError
from semantix.utils.lang import context as lang_context
from semantix.utils.lang import yaml
from semantix.utils.config.schema import Schema
from semantix.utils import abc, helper


__all__ = ['config', 'configurable', 'cvalue']


class ConfigError(SemantixError):
    pass

class ConfigRequiredValueError(ConfigError):
    pass

class ConfigAbstractValueError(ConfigError, AttributeError):
    # Derived from AttributeError on purpose - to not to screw up some code that
    # inspects class attributes with hasattr/getattr (for instance abc.ABCMeta)
    #
    pass


_Config_fields = ('_name', '_loaded_values', '_bound_to')
class _Config:
    # '~' -- is just a way to access the actual cvalue in the config tree.
    # Without it, __getattribute__ just returns a value of the cvalue you are
    # trying to access.  It's important to understand, that the '~' here is just
    # an access mechanism, not a way of storing data.
    #

    def __init__(self, name):
        self._name = name
        self._loaded_values = {}
        self._bound_to = None

    def __setattr__(self, name, value):
        if name not in _Config_fields \
                            and not isinstance(value, _Config) and not isinstance(value, cvalue):
            raise ConfigError('%s.%s is a read-only config property' % (self._name, name))
        object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        if name in ('__dict__', '__bases__') or name in _Config_fields:
            return object.__getattribute__(self, name)

        return_cvalue = False
        if name[0] == '~':
            name = name[1:]
            return_cvalue = True

        if name not in self.__dict__:
            raise AttributeError('%s.%s config property does not exist' % (self._name, name))

        if name in self.__dict__ and isinstance(self.__dict__[name], cvalue) and not return_cvalue:
            return self.__dict__[name]._get_value()
        else:
            return object.__getattribute__(self, name)

    def __iter__(self):
        result = []

        for name in self.__dict__:
            if name not in _Config_fields:
                if hasattr(self, '~' + name):
                    result.append((name, getattr(self, '~' + name)))
                else:
                    result.appned((name, getattr(self, name)))

        result.extend(self._loaded_values.items())
        return iter(sorted(result, key=lambda item: item[0]))


class _RootConfig(_Config):
    pass

config = _RootConfig('config')


def set_value(name, value, context=None):
    assert not isinstance(value, cvalue)

    parts = name.split('.')
    value_name = parts[-1]
    node = config

    for part in parts[:-1]:
        if hasattr(node, part) and isinstance(getattr(node, part), cvalue):
            raise ConfigError('Overlapping configs: %s.%s' % (node._name, part))

        if not hasattr(node, part):
            setattr(node, part, _Config(node._name + '.' + part))

        node = getattr(node, part)


    if hasattr(node, '~' + value_name):
        if isinstance(getattr(node, '~' + value_name), cvalue):
            getattr(node, '~' + value_name)._set_value(value, context)
        else:
            raise ConfigError('Overlapping configs: %s.%s' % (node._name, value_name))

    else:
        if node._bound_to and inspect.isclass(node._bound_to):
            # If we're setting a value for the config node that doesn't have such spot,
            # and the node is bound to some class, then try to find a base class that has
            # such spot, and if it has - add & reg a cvalue to the class that the current
            # node is bound to.  This essentially makes property inheritance work, even if
            # a child class doesn't define cvalue property explicitly, but its parent has it,
            # it's possible to configure this property via yaml or set_value call.
            #
            cls = node._bound_to

            if _inherit_cvalue(node._bound_to, value_name, value, context):
                _ensure_abstracts(node._bound_to)
                return

        node._loaded_values[value_name] = (value, context)


def get_cvalue(name):
    name = name.split('.')
    node = config

    for part in name[:-1]:
        if not hasattr(node, part):
            raise ConfigError('Unable to get %s cvalue due to the incorrect path' % \
                                                                                '.'.join(name))
        node = getattr(node, part)

    assert isinstance(getattr(node, '~' + name[-1]), cvalue)
    return getattr(node, '~' + name[-1])


def _get_conf(name, config=config):
    name = name.split('.')
    node = config
    for part in name:
        if not hasattr(node, part):
            return
        node = getattr(node, part)
        assert not isinstance(node, cvalue)
    return node


def _check_name(full_name:str, name:str):
    if name in _Config_fields:
        raise ConfigError('Unable to apply @configurable, invalid name: "%s", contains "%s"'
                          ' (which is in conflict with internal config implementation)' % \
                          (full_name, name))

@checktypes
def _set_dir(name:str, config=config):
    node = config
    parts = name.split('.')

    for part in parts:
        _check_name(name, part)

        if hasattr(node, part) and isinstance(getattr(node, '~' + part), cvalue):
            raise ConfigError('Overlapping configs: %s' % node._name)

        if not hasattr(node, part):
            setattr(node, part, _Config(node._name + '.' + part))

        node = getattr(node, part)

    return node


def _find_parent_cvalue(cls, name):
    for parent in cls.__mro__[1:]:
        if name in parent.__dict__ and isinstance(parent.__dict__[name], cvalue):
            return parent.__dict__[name]


def _inherit_cvalue(cls, name, value, value_ctx):
    shadow = _find_parent_cvalue(cls, name)

    if not shadow:
        return

    new = shadow._copy_for_override()

    if new._abstract:
        new._abstract = False

    new._set_value(value, value_ctx)

    if shadow._abstract and hasattr(cls, '__abstractmethods__'):
        cls.__abstractmethods__ = frozenset(cls.__abstractmethods__ - {name})

    new._set_name(cls.__module__ + '.' + cls.__name__ + '.' + name)
    setattr(cls, name, new)
    new._bound_to = cls

    return new


def _ensure_abstracts(cls):
    abstracts = set()
    nonabstracts = set()

    for c in reversed(cls.__mro__):
        for name, value in c.__dict__.items():
            if isinstance(value, cvalue):
                if value._abstract and name not in nonabstracts:
                    abstracts.add(name)
                if not value._abstract:
                    abstracts.discard(name)
                    nonabstracts.add(name)

    merged = getattr(cls, '__abstractmethods__', set())
    merged |= abstracts
    merged -= nonabstracts
    cls.__abstractmethods__ = frozenset(merged)


_Marker = object()
_std_type = type


def configurable(obj, *, basename=None, bind_to=None):
    if basename is None and obj.__module__ == '__main__':
        raise ConfigError('Unable to determine module\'s path')

    obj_name = basename or (obj.__module__ + '.' + obj.__name__)
    bind_to = obj if  bind_to is None else bind_to

    def decorate_function(obj):
        assert inspect.isfunction(obj)
        todecorate = False

        args_spec = get_argsspec(obj)
        checkers = FunctionValidator.get_checkers(obj, args_spec)

        defaults = itertools.chain(
                               zip(reversed(args_spec.args), reversed(args_spec.defaults)) \
                                                if args_spec.defaults else (),

                               args_spec.kwonlydefaults.items() \
                                                if args_spec.kwonlydefaults else ()
                             )

        for arg_name, arg_default in defaults:
            if isinstance(arg_default, cvalue):
                assert not arg_default.bound_to, 'argument was processed twice'

                todecorate = True

                if arg_default._abstract:
                    raise TypeError('Abstract cvalue may be defined only as a class property: ' \
                                    'got %r cvalue defined for %r function' % \
                                    (arg_name, obj.__name__))

                arg_default._set_name(obj_name + '.' + arg_name)
                arg_default._bind(bind_to)
                arg_default._owner = obj

                if arg_default._validator:
                    assert isinstance(arg_default._validator, Checker)

                    if arg_name in checkers:
                        checkers[arg_name] = CombinedChecker(checkers[arg_name],
                                                             arg_default._validator)
                        arg_default._validator = checkers[arg_name]
                    else:
                        checkers[arg_name] = arg_default._validator

                elif arg_name in checkers:
                    checker = checkers[arg_name]
                    arg_default._set_validator(checker)

                    if isinstance(checker, TypeChecker):
                        arg_default.type = checker.target

                arg_default._validate()

        if not todecorate:
            return obj

        def wrapper(*args, **kwargs):
            FunctionValidator.validate_kwonly(obj, args, args_spec)

            args = list(args)
            if args_spec.defaults:
                try:
                    flatten_args_spec = getattr(obj, '_flatten_args_spec_')
                except AttributeError:
                    flatten_args_spec = tuple(enumerate(reversed(tuple(
                                            itertools.zip_longest(
                                                reversed(tuple(args_spec.args) \
                                                                    if args_spec.args else []),

                                                reversed(tuple(args_spec.defaults) \
                                                                    if args_spec.defaults else []),

                                                fillvalue=_Marker
                                            )
                                        ))))

                    setattr(obj, '_flatten_args_spec_', flatten_args_spec)

                for i, (arg_name, default) in flatten_args_spec:
                    if i >= len(args):
                        if default is _Marker:
                            raise TypeError('%s argument is required' % arg_name)

                        if isinstance(default, cvalue):
                            while isinstance(default, cvalue):
                                default = default._get_value()
                        args.append(default)

            if args_spec.kwonlydefaults:
                for def_name, def_value in args_spec.kwonlydefaults.items():
                    if not def_name in kwargs and isinstance(def_value, cvalue):
                        while isinstance(def_value, cvalue):
                            def_value = def_value._get_value()
                        kwargs[def_name] = def_value

            FunctionValidator.check_args(obj, args, kwargs, args_spec, checkers)
            return obj(*args, **kwargs)

        decorate(wrapper, obj)

        _conf = _get_conf(obj_name)
        if _conf:
            _conf._bound_to = obj

        return wrapper

    def decorate_class(obj):
        assert inspect.isclass(obj)
        todecorate = False

        def _decorate(wrapped):
            return configurable(wrapped, basename=obj_name + '.' + wrapped.__name__, bind_to=obj)

<<<<<<< HEAD
        for attr_name, attr_value in tuple(obj.__dict__.items()):
=======
        for attr_name, attr_value in obj.__dict__.items():
>>>>>>> utils.config: Abstract cvalues, inheritance support.
            if isinstance(attr_value, cvalue):
                assert not attr_value.bound_to

                if attr_value._abstract:
                    abc.push_abstract(obj, attr_name)

                if attr_value._inherits:
                    target = _find_parent_cvalue(obj, attr_name)

                    if not target:
                        raise TypeError('Unable to find base cvalue to be inherited from')

                    if not attr_value._validator:
                        attr_value._validator = target._validator
                        attr_value._type = target._type

                    if attr_value._doc is None:
                        attr_value._doc = target._doc

                    if attr_value._doc is None:
                        attr_value._doc = target._doc

                attr_value._set_name(obj_name + '.' + attr_name)
                attr_value._bind(bind_to)
                attr_value._owner = obj
                attr_value._validate()
                todecorate = True

            else:
                patched = apply_decorator(attr_value, decorate_function=_decorate,
                                          decorate_class=_decorate)

                if patched is not attr_value:
                    todecorate = True
                    setattr(obj, attr_name, patched)

        if not todecorate:
            # Even if there is no cvalues found in the class being @configurable, perhaps
            # it has some @configurable parents, and if it is, then we have to decorate it
            # too, in order to be able to set inherited values for it.
            #
            todecorate = any(_get_conf(cls.__module__ + '.' + cls.__name__) \
                                                                    for cls in obj.__mro__[1:])

        if todecorate:
            _conf = _get_conf(obj_name)
            if not _conf:
                _conf = _set_dir(obj_name)
            else:
                # Now, if let's say we imported a yaml config with some values, which created
                # a bunch of unbound values which may be overrides for parent cvalues, we need
                # to try to walk the whole inheritance tree and match those cvalues.
                #
                for attr_name, (attr_value, attr_value_ctx) in list(_conf._loaded_values.items()):
                    # if `_inherit_cvalue` finds a parent cvalue, then it copies it and calls
                    # `cvalue._set_value` which in turn cleans up relative `_loaded_values`
                    #
                    _inherit_cvalue(obj, attr_name, attr_value, attr_value_ctx)

            _conf._bound_to = obj
            _ensure_abstracts(obj)

        return obj

    return apply_decorator(obj, decorate_function=decorate_function, decorate_class=decorate_class)


class NoDefault:
    def __str__(self):
        return '<config.NoDefault>'
    __repr__ = __str__
NoDefault = NoDefault()


class cvalue(ChecktypeExempt):
    __slots__ = ('_name', '_default', '_value', '_value_context', '_doc', '_validator', '_type',
                 '_bound_to', '_inter_cache', '_owner', '_required', '_abstract', '_inherits')

    _inter_re = re.compile(r'''(?P<text>[^$]+) |
                               (?P<ref>\${    (?P<to>[^}]+)   })''', re.M | re.X)

    def __init__(self, default=NoDefault, *, doc=None, validator=None, type=None,
                 abstract=False, inherits=False):

        if abstract and default is not NoDefault:
            raise TypeError('Unable to set default for abstract cvalue')

        self._name = None
        self._value = self._default = default
        self._value_context = None
        self._doc = doc
        self._type = type
        self._bound_to = None
        self._owner = None
        self._inter_cache = None
        self._abstract = abstract
        self._inherits = inherits

        if validator:
            if not isinstance(validator, Checker):
                validator = Checker.get(validator)

            if type and isinstance(type, _std_type):
                validator = CombinedChecker(TypeChecker(type), validator)

            self._validator = validator

        elif type and isinstance(type, _std_type):
            self._validator = TypeChecker(type)

        else:
            self._validator = None

    doc = property(lambda self: self._doc)
    bound_to = property(lambda self: self._bound_to)
    default = property(lambda self: self._default)
    required = property(lambda self: self._default is NoDefault)

    def _copy_for_override(self):
        return cvalue(self._value, doc=self.doc, validator=self._validator,
                      type=self._type, abstract=self._abstract)

    def _bind(self, owner):
        self._bound_to = owner

    def _set_type(self, type_):
        assert isinstance(type_, type)
        self._type = type_

    type = property(lambda self: self._type, _set_type)

    def _get_value(self):
        if self._abstract:
            raise ConfigAbstractValueError('Abstract cvalue %r must be overrided' % self._name)

        if self._value is NoDefault:
            raise ConfigRequiredValueError('%r is a required config setting' % self._name)

        if 0 and isinstance(self._value, str) and '$' in self._value:
            if not self._inter_cache:
                matches = cvalue._inter_re.findall(self._value)
                check = ''.join(m[0]+m[1] for m in matches)

                if check != self._value:
                    raise ConfigError('Malformed substitution syntax: "%s"%s' % \
                                      (self._value, self._value_context if self._value_context \
                                                                                           else ''))
                self._inter_cache = []
                for match in matches:
                    if match[0]:
                        self._inter_cache.append(match[0])
                    else:
                        self._inter_cache.append(get_cvalue(match[2]))

            return ''.join(item._get_value() if isinstance(item, cvalue) else item \
                                                                    for item in self._inter_cache)

        return self._value

    def _set_value(self, value, value_context=None):
        if self._abstract:
            raise TypeError('Unable to set value for abstract cvalue %r' % self._name)

        self._value = value
        self._value_context = value_context
        self._inter_cache = None
        self._validate()

    @checktypes
    def _set_name(self, name:str):
        self._name = name

        dir_name, _, self_name = name.rpartition('.')

        _check_name(name, self_name)

        node = _set_dir(dir_name)

        if hasattr(node, self_name) and isinstance(getattr(node, '~' + self_name), cvalue):
            raise ConfigError('Overlapping configs: %s' % self._name)

        setattr(node, self_name, self)

        if self_name in node._loaded_values:
            self._set_value(*node._loaded_values[self_name])
            del node._loaded_values[self_name]

    @checktypes
    def _set_validator(self, validator:Checker):
        self._validator = validator

    def _validate(self):
        if self._validator and self._value is not NoDefault:
            try:
                FunctionValidator.check_value(self._validator, self._value,
                                              self._name, 'default config value')
            except TypeError as ex:
                raise TypeError('Invalid value %r for "%s" config option%s' % \
                                (self._value, self._name,
                                 ' in "%s"' % self._value_context if self._value_context else '')) \
                                                                                            from ex

    def __get__(self, instance, owner):
        return self._get_value()

    def __set__(self, instance, value):
        raise TypeError('%s is a read-only config property' % self._name)

    def __delete__(self, instance):
        raise TypeError('%s is a read-only config property' % self._name)

    def __repr__(self):
        return "<cvalue at 0x%x value:%r>" % (id(self), self._value)


class _YamlObject(yaml.Object):
    def __sx_setstate__(self, data):
        object.__setattr__(self, '__sx_yaml_data__', data)


class _Loader(_YamlObject):
    @staticmethod
    def traverse(obj, name=''):
        if isinstance(obj, _YamlObject):
            data = object.__getattribute__(obj, '__sx_yaml_data__')

            if isinstance(data, dict):
                for key in data:
                    _Loader.traverse(data[key], (name + '.' + key) if name else key)
            else:
                context = lang_context.SourceContext.from_object(obj)
                set_value(name, data, str(context))

    def __sx_setstate__(self, data):
        super().__sx_setstate__(data)
        self.traverse(self)
