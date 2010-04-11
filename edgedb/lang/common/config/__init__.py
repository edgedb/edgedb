##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import inspect

from semantix.utils.functional import decorate, BaseDecorator
from semantix.utils.functional.types import Checker, FunctionValidator, checktypes, ChecktypeExempt
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

        super().__setattr__(name, value)

    def __getattribute__(self, name):
        if name in ('__dict__', '__bases__', '_name', '_set_value'):
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

    def _set_value(self, name, value, context=None):
        """
        XXX: call only on the top-level config object
        """
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
                print('>>>', node._name, getattr(node, '~' + name[-1]), getattr(node, '~' + name[-1])._name)
                raise ConfigError('Overlapping configs: %s.%s' % (node._name, name[-1]))

        else:
            node._loaded_values[name[-1]] = (value, context)

config = _Config('config')


def configurable(obj, *, _basename=None, _bind_to=None):
    obj_name = _basename or (obj.__module__ + '.' + obj.__name__)
    bind_to = _bind_to or obj

    if inspect.isfunction(obj):
        args_spec = FunctionValidator.get_argsspec(obj)
        checkers = FunctionValidator.get_checkers(obj, args_spec)

        defaults = []
        if args_spec.defaults:
            defaults.append(zip(reversed(args_spec.args), reversed(args_spec.defaults)))

        if args_spec.kwonlydefaults:
            defaults.append(args_spec.kwonlydefaults.items())

        for arg_name, arg_default in itertools.chain(*defaults):
            if isinstance(arg_default, carg) and not arg_default.bound_to:
                arg_default._set_name(obj_name + '.' + arg_name)
                arg_default._bind(bind_to)

                if not arg_default.validator and arg_name in checkers:
                    arg_default._set_validator(checkers[arg_name])

                arg_default._validate()


        def wrapper(*args, **kwargs):
            FunctionValidator.validate_kwonly(obj, args, args_spec)

            args = list(args)

            j = 0
            for i, arg_name in enumerate(args_spec.args):
                if i >= len(args):
                    if isinstance(args_spec.defaults[j], carg):
                        args.append(args_spec.defaults[j]._get_value())
                    j += 1

            if args_spec.kwonlydefaults:
                for def_name, def_value in args_spec.kwonlydefaults.items():
                    if not def_name in kwargs and isinstance(def_value, carg):
                        kwargs[def_name] = def_value._get_value()

            return obj(*args, **kwargs)

        decorate(wrapper, obj)
        return wrapper

    elif inspect.isclass(obj):
        for attr_name, attr_value in obj.__dict__.items():
            if isinstance(attr_value, cvar) and not attr_value.bound_to:
                attr_value._set_name(obj_name + '.' + attr_name)
                attr_value._bind(bind_to)
                attr_value._validate()

            elif inspect.isfunction(attr_value):
                setattr(obj, attr_name, configurable(attr_value, \
                                                     _basename=obj_name + '.' + attr_value.__name__,
                                                     _bind_to=obj))

            elif isinstance(attr_value, classmethod):
                setattr(obj, attr_name, classmethod(configurable(attr_value.__func__, \
                                           _basename=obj_name + '.' + attr_value.__func__.__name__,
                                           _bind_to=obj)))

            elif isinstance(attr_value, staticmethod):
                setattr(obj, attr_name, staticmethod(configurable(attr_value.__func__, \
                                           _basename=obj_name + '.' + attr_value.__func__.__name__,
                                           _bind_to=obj)))

            elif isinstance(attr_value, BaseDecorator):
                func = attr_value
                while isinstance(func, BaseDecorator):
                    host = func
                    func = attr_value._func_
                host._func_ = configurable(func, \
                                           _basename=obj_name + '.' + func.__name__,
                                           _bind_to=obj)

        return obj

    else:
        raise ConfigError('@configurable: only classes and functions are supported')


class cvalue(ChecktypeExempt):
    __slots__ = ('name', 'value', 'value_context', 'doc', 'validator', 'type', 'bound_to')

    def __init__(self, default=None, *, doc=None, validator=None, type=None):
        self.name = None
        self.value = default
        self.value_context = None
        self.doc = doc
        self.type = type
        self.bound_to = None

        self.validator = None
        if validator:
            if isinstance(validator, Checker):
                self._set_validator(validator)
            else:
                self._set_validator(Checker.get(validator))

    def _bind(self, owner):
        self.bound_to = owner

    def _get_value(self):
        return self.value

    def _set_value(self, value, context=None):
        self.value = value
        self.value_context = context
        self._validate()

    @checktypes
    def _set_name(self, name:str):
        self.name = name

        node = config
        name = name.split('.')
        for part in name[:-1]:
            if hasattr(node, part) and isinstance(getattr(node, '~' + part), cvalue):
                raise ConfigError('Overlapping configs: %s' % node._name)

            if not hasattr(node, part):
                setattr(node, part, _Config(node._name + '.' + part))

            node = getattr(node, part)

        if hasattr(node, name[-1]) and isinstance(getattr(node, '~' + name[-1]), cvalue):
            raise ConfigError('Overlapping configs: %s' % self.name)

        setattr(node, name[-1], self)

        if name[-1] in node._loaded_values:
            self._set_value(*node._loaded_values[name[-1]])
            del node._loaded_values[name[-1]]

    @checktypes
    def _set_validator(self, validator:Checker):
        self.validator = validator

    def _validate(self):
        if self.validator:
            try:
                FunctionValidator.check_value(self.validator, self.value,
                                              self.name, 'default config value')
            except TypeError as ex:
                raise TypeError('Invalid value %r for "%s" config option%s' % \
                                (self.value, self.name,
                                 ' in "%s"' % self.value_context if self.value_context else '')) \
                                                                                            from ex


class carg(cvalue):
    __slots__ = ()


class cvar(cvalue):
    __slots__ = ()

    def __get__(self, instance, owner):
        return self._get_value()

    def __set__(self, instance, value):
        raise TypeError('%s is a read-only config property' % self.name)

    def __delete__(self, instance):
        raise TypeError('%s is a read-only config property' % self.name)


class _Loader(yaml.Object):
    @staticmethod
    def traverse(obj, name=''):
        if isinstance(obj, yaml.Object):
            if isinstance(obj.data, dict):
                for key in obj.data:
                    _Loader.traverse(obj.data[key], (name + '.' + key) if name else key)
            else:
                config._set_value(name, obj.data, str(obj.context))

    def construct(self):
        self.traverse(self)
