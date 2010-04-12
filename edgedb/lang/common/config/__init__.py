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

from semantix.utils.functional import decorate, BaseDecorator
from semantix.utils.functional.types import Checker, FunctionValidator, checktypes, \
                                            ChecktypeExempt, TypeChecker
from semantix.exceptions import SemantixError
from semantix.utils.lang import yaml
from semantix.utils.config.schema import Schema


__all__ = ['ConfigError', 'ConfigRequiredValueError', 'config', 'configurable', 'cvalue']


class ConfigError(SemantixError):
    pass

class ConfigRequiredValueError(ConfigError):
    pass


_Config_fields = ('_name', '_loaded_values', '_bound_to')
class _Config:
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
            raise ConfigError('%s.%s config property does not exist' % (self._name, name))

        if name in self.__dict__ and isinstance(self.__dict__[name], cvalue) and not return_cvalue:
            return self.__dict__[name]._get_value()

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

    name = name.split('.')
    node = config

    for part in name[:-1]:
        if hasattr(node, part) and isinstance(getattr(node, part), cvalue):
            raise ConfigError('Overlapping configs: %s.%s' % (node._name, part))

        if not hasattr(node, part):
            setattr(node, part, _Config(node._name + '.' + part))

        node = getattr(node, part)

    if hasattr(node, '~' + name[-1]):
        if isinstance(getattr(node, '~' + name[-1]), cvalue):
            getattr(node, '~' + name[-1])._set_value(value, context)

        else:
            raise ConfigError('Overlapping configs: %s.%s' % (node._name, name[-1]))

    else:
        node._loaded_values[name[-1]] = (value, context)


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


def _get_conf(config, name):
    name = name.split('.')
    node = config
    for part in name:
        if not hasattr(node, part):
            return
        node = getattr(node, part)
        assert not isinstance(node, cvalue)
    return node


def configurable(obj, *, basename=None, bind_to=None):
    if basename is None and obj.__module__ == '__main__':
        raise ConfigError('Unable to determine module\'s path')

    obj_name = basename or (obj.__module__ + '.' + obj.__name__)
    bind_to = bind_to or obj

    def decorate_function(obj):
        assert inspect.isfunction(obj)
        todecorate = False

        args_spec = FunctionValidator.get_argsspec(obj)
        checkers = FunctionValidator.get_checkers(obj, args_spec)

        defaults = []
        if args_spec.defaults:
            defaults.append(zip(reversed(args_spec.args), reversed(args_spec.defaults)))

        if args_spec.kwonlydefaults:
            defaults.append(args_spec.kwonlydefaults.items())

        for arg_name, arg_default in itertools.chain(*defaults):
            if isinstance(arg_default, cvalue) and not arg_default.bound_to:
                todecorate = True

                arg_default._set_name(obj_name + '.' + arg_name)
                arg_default._bind(bind_to)
                arg_default._owner = obj

                if not arg_default._validator and arg_name in checkers:
                    checker = checkers[arg_name]
                    arg_default._set_validator(checker)

                    if isinstance(checker, TypeChecker):
                        arg_default.type = checker.target

                arg_default._validate()

        if todecorate:
            _conf = _get_conf(config, obj_name)
            if _conf:
                _conf._bound_to = obj
        else:
            return obj

        def wrapper(*args, **kwargs):
            FunctionValidator.validate_kwonly(obj, args, args_spec)

            args = list(args)

            if args_spec.defaults:
                j = 0
                for i, arg_name in enumerate(args_spec.args):
                    if i >= len(args):
                        default = args_spec.defaults[j]
                        if isinstance(default, cvalue):
                            while isinstance(default, cvalue):
                                default = default._get_value()
                        args.append(default)
                        j += 1

            if args_spec.kwonlydefaults:
                for def_name, def_value in args_spec.kwonlydefaults.items():
                    if not def_name in kwargs and isinstance(def_value, cvalue):
                        while isinstance(def_value, cvalue):
                            def_value = def_value._get_value()
                        kwargs[def_name] = def_value

            return obj(*args, **kwargs)

        decorate(wrapper, obj)
        return wrapper

    def decorate_class(obj):
        assert inspect.isclass(obj)
        todecorate = False

        def _decorate(wrapped):
            return configurable(wrapped, basename=obj_name + '.' + wrapped.__name__, bind_to=obj)

        for attr_name, attr_value in obj.__dict__.items():
            if isinstance(attr_value, cvalue) and not attr_value.bound_to:
                attr_value._set_name(obj_name + '.' + attr_name)
                attr_value._bind(bind_to)
                attr_value._owner = obj

                if attr_value.type and isinstance(attr_value.type, type):
                    attr_value._validator = Checker.get(attr_value.type)

                attr_value._validate()
                todecorate = True

            else:
                patched = FunctionValidator.try_apply_decorator(attr_value, _decorate, _decorate)

                if patched is not attr_value:
                    todecorate = True
                    setattr(obj, attr_name, patched)

        if todecorate:
            _conf = _get_conf(config, obj_name)
            if _conf:
                _conf._bound_to = obj

        return obj

    return FunctionValidator.try_apply_decorator(obj, decorate_function, decorate_class)


class _NoDefault:
    pass
NoDefault = _NoDefault()


class cvalue(ChecktypeExempt):
    __slots__ = ('_name', '_default', '_value', '_value_context', '_doc', '_validator', '_type',
                 '_bound_to', '_inter_cache', '_owner', '_required')

    _inter_re = re.compile(r'''(?P<text>[^$]+) |
                               (?P<ref>\${    (?P<to>[^}]+)   })''', re.M | re.X)

    def __init__(self, default=NoDefault, *, doc=None, validator=None, type=None):
        self._name = None
        self._value = self._default = default
        self._value_context = None
        self._doc = doc
        self._type = type
        self._bound_to = None
        self._owner = None
        self._inter_cache = None

        self._validator = None
        if validator:
            if isinstance(validator, Checker):
                self._set_validator(validator)
            else:
                self._set_validator(Checker.get(validator))

    doc = property(lambda self: self._doc)
    bound_to = property(lambda self: self._bound_to)
    default = property(lambda self: self._default)
    required = property(lambda self: self._default is NoDefault)

    def _bind(self, owner):
        self._bound_to = owner

    def _set_type(self, type_):
        assert isinstance(type_, type)
        self._type = type_

    type = property(lambda self: self._type, _set_type)

    def _get_value(self):
        if self._value is NoDefault:
            raise ConfigRequiredValueError('%s is a required config setting' % self._name)

        if isinstance(self._value, str) and '$' in self._value:
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

            return ''.join(item._get_value() if isinstance(item, cvalue) else item\
                                                                    for item in self._inter_cache)

        return self._value

    def _set_value(self, value, context=None):
        self._value = value
        self._value_context = context
        self._inter_cache = None
        self._validate()

    @checktypes
    def _set_name(self, name:str):
        self._name = name

        node = config
        name = name.split('.')

        for part in name:
            if part in _Config_fields:
                raise ConfigError('Unable to apply @configurable, invalid name: "%s", contains "%s"'
                                  ' (which is in conflict with internal config implementation)' % \
                                  ('.'.join(name), part))

        for part in name[:-1]:
            if hasattr(node, part) and isinstance(getattr(node, '~' + part), cvalue):
                raise ConfigError('Overlapping configs: %s' % node._name)

            if not hasattr(node, part):
                setattr(node, part, _Config(node._name + '.' + part))

            node = getattr(node, part)

        if hasattr(node, name[-1]) and isinstance(getattr(node, '~' + name[-1]), cvalue):
            raise ConfigError('Overlapping configs: %s' % self._name)

        setattr(node, name[-1], self)

        if name[-1] in node._loaded_values:
            self._set_value(*node._loaded_values[name[-1]])
            del node._loaded_values[name[-1]]

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


class _Loader(yaml.Object):
    @staticmethod
    def traverse(obj, name=''):
        if isinstance(obj, yaml.Object):
            if isinstance(obj.data, dict):
                for key in obj.data:
                    _Loader.traverse(obj.data[key], (name + '.' + key) if name else key)
            else:
                set_value(name, obj.data, str(obj.context))

    def construct(self):
        self.traverse(self)
