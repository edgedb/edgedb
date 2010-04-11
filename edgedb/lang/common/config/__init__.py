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


class ConfigError(SemantixError):
    pass


class _Config:
    def __init__(self, name):
        self._name = name
        self._loaded_values = {}

    def __setattr__(self, name, value):
        if name not in ('_name', '_loaded_values') \
                            and not isinstance(value, _Config) and not isinstance(value, cvalue):
            raise ConfigError('%s.%s is a read-only config property' % (self._name, name))
        object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        if name in ('__dict__', '__bases__', '_name'):
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


class _RootConfig(_Config):
    def __getattribute__(self, name):
        if name in ('__dict__', '__bases__', '_name', 'set_value', 'cvalue'):
            return object.__getattribute__(self, name)
        return _Config.__getattribute__(self, name)

    def set_value(self, name, value, context=None):
        assert not isinstance(value, cvalue)

        name = name.split('.')
        node = self

        for part in name[:-1]:
            if hasattr(node, part) and isinstance(getattr(node, part), cvalue):
                raise ConfigError('Overlapping configs: %s.%s' % (node._name, part))

            if not hasattr(node, part):
                setattr(node, part, _Config(node._name + '.' + part))

            node = getattr(node, part)

        if hasattr(node, name[-1]):
            if isinstance(getattr(node, '~' + name[-1]), cvalue):
                getattr(node, '~' + name[-1])._set_value(value, context)

            else:
                raise ConfigError('Overlapping configs: %s.%s' % (node._name, name[-1]))

        else:
            node._loaded_values[name[-1]] = (value, context)

    def cvalue(self, name):
        name = name.split('.')
        node = self

        for part in name[:-1]:
            if not hasattr(node, part):
                raise ConfigError('Unable to get %s cvalue due to the incorrect path' % \
                                                                                    '.'.join(name))
            node = getattr(node, part)

        assert isinstance(getattr(node, '~' + name[-1]), cvalue)
        return getattr(node, '~' + name[-1])

config = _RootConfig('config')


def configurable(obj, *, basename=None, bind_to=None):
    if basename is None and obj.__module__ == '__main__':
        raise ConfigError('Unable to determine module\'s path')

    obj_name = basename or (obj.__module__ + '.' + obj.__name__)
    bind_to = bind_to or obj

    def decorate_function(obj):
        assert inspect.isfunction(obj)

        args_spec = FunctionValidator.get_argsspec(obj)
        checkers = FunctionValidator.get_checkers(obj, args_spec)

        defaults = []
        if args_spec.defaults:
            defaults.append(zip(reversed(args_spec.args), reversed(args_spec.defaults)))

        if args_spec.kwonlydefaults:
            defaults.append(args_spec.kwonlydefaults.items())

        for arg_name, arg_default in itertools.chain(*defaults):
            if isinstance(arg_default, cvalue) and not arg_default.bound_to:
                arg_default._set_name(obj_name + '.' + arg_name)
                arg_default._bind(bind_to)

                if not arg_default._validator and arg_name in checkers:
                    checker = checkers[arg_name]
                    arg_default._set_validator(checker)

                    if isinstance(checker, TypeChecker):
                        arg_default.type = checker.target

                arg_default._validate()


        def wrapper(*args, **kwargs):
            FunctionValidator.validate_kwonly(obj, args, args_spec)

            args = list(args)

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

        def _decorate(wrapped):
            return configurable(wrapped, basename=obj_name + '.' + wrapped.__name__, bind_to=obj)

        for attr_name, attr_value in obj.__dict__.items():
            if isinstance(attr_value, cvalue) and not attr_value.bound_to:
                attr_value._set_name(obj_name + '.' + attr_name)
                attr_value._bind(bind_to)

                if attr_value.type and isinstance(attr_value.type, type):
                    attr_value._validator = Checker.get(attr_value.type)

                attr_value._validate()

            else:
                patched = FunctionValidator.try_apply_decorator(attr_value, _decorate, _decorate)

                if patched is not attr_value:
                    setattr(obj, attr_name, patched)
        return obj

    return FunctionValidator.try_apply_decorator(obj, decorate_function, decorate_class)


class cvalue(ChecktypeExempt):
    __slots__ = ('_name', '_default', '_value', '_value_context', '_doc', '_validator', '_type',
                 '_bound_to', '_inter_cache')

    _inter_re = re.compile(r'''(?P<text>[^$]+) |
                               (?P<ref>\${    (?P<to>[^}]+)   })''', re.M | re.X)

    def __init__(self, default=None, *, doc=None, validator=None, type=None):
        self._name = None
        self._value = self._default = default
        self._value_context = None
        self._doc = doc
        self._type = type
        self._bound_to = None
        self._inter_cache = None

        self._validator = None
        if validator:
            if isinstance(validator, Checker):
                self._set_validator(validator)
            else:
                self._set_validator(Checker.get(validator))

    doc = property(lambda self: self._doc)
    bound_to = property(lambda self: self._bound_to)

    def _bind(self, owner):
        self._bound_to = owner

    def _set_type(self, type_):
        assert isinstance(type_, type)
        self._type = type_

    type = property(lambda self: self._type, _set_type)

    def _get_value(self):
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
                        self._inter_cache.append(config.cvalue(match[2]))

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
        if self._validator:
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
                config.set_value(name, obj.data, str(obj.context))

    def construct(self):
        self.traverse(self)
