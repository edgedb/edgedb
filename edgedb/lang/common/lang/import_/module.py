##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections.abc
import importlib
import types


class Module(types.ModuleType):
    pass


class BaseProxyModule:
    def __init__(self, name, module):
        self.__name__ = name
        self.__wrapped__ = module


class LightProxyModule(BaseProxyModule):
    """Light ProxyModule object, does not keep track of wrapped
    module's attributes, so if there are any references to them in
    the code then it may be broken after reload.
    """

    def __setattr__(self, name, value):
        if name not in ('__name__', '__wrapped__'):
            return setattr(self.__wrapped__, name, value)
        else:
            return object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        if name in ('__name__', '__repr__', '__wrapped__'):
            return object.__getattribute__(self, name)

        wrapped = object.__getattribute__(self, '__wrapped__')
        return getattr(wrapped, name)

    def __delattr__(self, name):
        if name in ('__name__', '__repr__', '__wrapped__'):
            return object.__delattr__(self, name)

        wrapped = object.__getattribute__(self, '__wrapped__')
        return delattr(wrapped, name)

    def __repr__(self):
        return "<{} {!r} from {!r}>".format(object.__getattribute__(self, '__class__').__name__,
                                            self.__name__, self.__wrapped__.__file__)


class AutoloadingLightProxyModule(LightProxyModule):
    def __init__(self, name, module=None):
        if module is None:
            module = importlib.import_module(name)
        super().__init__(name, module)

    def __getattribute__(self, name):
        if name in ('__reduce_ex__'):
            return object.__getattribute__(self, name)
        else:
            return super().__getattribute__(name)

    def __reduce_ex__(self, version):
        return type(self), (self.__name__,)


class ClassProxyMeta(type):
    """Base metaclass used for class proxies"""

    def __new__(mcls, name, bases, dct):
        if bases:
            # Make sure that this class is one and only proxy in MRO
            b = []
            for base in bases:
                try:
                    base = base.__wrapped__
                except AttributeError:
                    pass
                b.append(base)
            bases = tuple(b)

        orig_mcls = type if mcls is ClassProxyMeta else mcls.__mro__[2]
        # Only use ClassProxyMeta for actual proxies
        target_mcls = mcls if name.endswith('_sx_proxy__') else orig_mcls
        return orig_mcls.__new__(target_mcls, name, bases, dct)

    def __subclasscheck__(cls, subclass):
        # Make sure that proxied classes are transparent to isinstance and issubclass
        wrapped = object.__getattribute__(cls, '__wrapped__')
        if wrapped is None:
            raise TypeError('proxy module attribute no longer exists')

        return type.__subclasscheck__(wrapped, subclass)


class BaseObjectProxy:
    """A common set of magic that is used both by Object and Class proxies"""

    def __getattribute__(self, name):
        if name in ('__repr__', '__wrapped__'):
            return object.__getattribute__(self, name)

        wrapped = object.__getattribute__(self, '__wrapped__')

        if wrapped is None:
            raise TypeError('proxy module attribute no longer exists: {}'.format(name))

        return getattr(wrapped, name)

    def __setattr__(self, name, value):
        if name == '__wrapped__':
            return object.__setattr__(self, name, value)

        wrapped = object.__getattribute__(self, '__wrapped__')
        if wrapped is None:
            raise TypeError('proxy module attribute no longer exists: {}'.format(name))

        return setattr(wrapped, name, value)

    def __call__(self, *args, **kwargs):
        wrapped = object.__getattribute__(self, '__wrapped__')
        if wrapped is None:
            raise TypeError('proxy module attribute no longer exists')

        return wrapped.__call__(*args, **kwargs)

    def __new_object__(cls, *args, **kwargs):
        wrapped = object.__getattribute__(cls, '__wrapped__')
        if wrapped is None:
            raise TypeError('proxy module attribute no longer exists')

        self = wrapped.__new__(wrapped, *args, **kwargs)
        self.__init__(*args, **kwargs)

        return self


class ObjectProxyMeta(type):
    _magicmethods = [
        '__abs__', '__add__', '__and__', '__call__',
        '__contains__', '__delitem__', '__div__', '__divmod__',
        '__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__',
        '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__',
        '__idiv__', '__idivmod__', '__ifloordiv__', '__ilshift__', '__imod__',
        '__imul__', '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__',
        '__isub__', '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__',
        '__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__',
        '__neg__', '__next__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__',
        '__rand__', '__rdiv__', '__rdivmod__', '__reduce__', '__reduce_ex__',
        '__repr__', '__reversed__', '__rfloorfiv__', '__rlshift__', '__rmod__',
        '__rmul__', '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
        '__rtruediv__', '__rxor__', '__setitem__', '__sub__',
        '__truediv__', '__xor__', '__str__'
    ]

    def __sx_adjust_class_dict__(cls, wrappedcls, dct=None):
        avail_methods = []

        def make_magicmethod(name):
            def method(self, *args, **kwargs):
                wrapped = object.__getattribute__(self, '__wrapped__')
                if wrapped is None:
                    raise TypeError('underlying module attribute {!r} no longer exists'.format(name))

                try:
                    orig_method = getattr(wrapped, name)
                except AttributeError:
                    raise TypeError('object {!r} has no {!r}'.format(wrapped, name))
                else:
                    return orig_method(*args, **kwargs)

            return method

        for methodname in cls._magicmethods:
            if hasattr(wrappedcls, methodname):
                if dct is None:
                    setattr(cls, methodname, make_magicmethod(methodname))
                else:
                    dct[methodname] = make_magicmethod(methodname)
                avail_methods.append(methodname)
            else:
                if dct is None:
                    try:
                        delattr(cls, methodname)
                    except AttributeError:
                        pass
                else:
                    dct.pop(methodname, None)

        if dct is None:
            setattr(cls, '__sx_proxy_signature__', hash(frozenset(avail_methods)))
        else:
            dct['__sx_proxy_signature__'] = hash(frozenset(avail_methods))

    def __sx_derive_class_for__(cls, wrappedcls):
        dct = {}
        cls.__sx_adjust_class_dict__(wrappedcls, dct)
        return cls.__class__(cls.__name__, (cls,), dct)


class ObjectProxy(BaseObjectProxy, metaclass=ObjectProxyMeta):
    """Common proxy for objects"""

    @classmethod
    def __sx_new__(cls, wrapped):
        newcls = cls.__sx_derive_class_for__(wrapped.__class__)
        self = newcls()
        object.__setattr__(self, '__wrapped__', wrapped)
        return self

    def __setattr__(self, name, value):
        if name == '__wrapped__':
            type(self).__sx_adjust_class_dict__(value.__class__)
        super().__setattr__(name, value)


def _proxy_class(mod, cls):
    if cls.__module__ != mod.__name__:
        # Only proxy classes actually defined in the module.
        return cls

    try:
        cls = cls.__wrapped__
    except AttributeError:
        pass

    try:
        metaclasses = object.__getattribute__(mod, '__sx_metaclsproxies__')
    except AttributeError:
        metaclasses = {}
        object.__setattr__(mod, '__sx_metaclsproxies__', metaclasses)

    result_metacls = type(cls)
    result_metacls_name = '{}.{}'.format(result_metacls.__module__,
                                         result_metacls.__name__)

    try:
        proxy_metacls = metaclasses[result_metacls_name]
    except KeyError:
        # Override metaclass to alter __new__ and __subclasscheck__ behaviour.
        if result_metacls is type:
            proxy_metacls = ClassProxyMeta
        else:
            proxy_metacls = type('{}_sx_proxy_mcls__'.format(result_metacls.__name__),
                                 (ClassProxyMeta, result_metacls),
                                 {'__module__': result_metacls.__module__})

        metaclasses[result_metacls_name] = proxy_metacls

    # Inject magic methods directly instead of inheriting from BaseProxy to avoid conflicts
    proxy = proxy_metacls('{}_sx_proxy__'.format(cls.__name__), (cls,),
                          {'__module__': cls.__module__,
                           '__getattribute__': BaseObjectProxy.__getattribute__,
                           '__setattr__': BaseObjectProxy.__setattr__,
                           '__new__': BaseObjectProxy.__new_object__,
                           '__wrapped__': cls
                          })

    return proxy


class ProxyModuleDict(collections.abc.MutableMapping):
    def __init__(self, proxy_module):
        self._proxy_module = proxy_module

    def __getitem__(self, key):
        return getattr(self._proxy_module, key)

    def __setitem__(self, key, value):
        return setattr(self._proxy_module, key, value)

    def __delitem__(self, key):
        return delattr(self._proxy_module, key)

    def __iter__(self):
        wrapped = object.__getattribute__(self._proxy_module, '__wrapped__')
        return iter(object.__getattribute__(wrapped, '__dict__'))

    def __len__(self):
        wrapped = object.__getattribute__(self._proxy_module, '__wrapped__')
        return len(object.__getattribute__(wrapped, '__dict__'))


class ProxyModule(LightProxyModule):
    """A module proxy that manages module attributes.

    This module proxy, besides encapsulating the module itself, also inserts
    a proxy for each top-level attribute in the module.

    Note: attributes other than top-level are not tracked.  Instances of proxied
          classes are not proxies.
    """

    def __getattribute__(self, name):
        # Create proxies when _reading_ attributes written directly into module's __dict__

        if name == '__dict__':
            try:
                dictproxy = object.__getattribute__(self, '__sx_dictproxy__')
            except AttributeError:
                dictproxy = ProxyModuleDict(self)
                object.__setattr__(self, '__sx_dictproxy__', dictproxy)

            return dictproxy

        result = super().__getattribute__(name)

        if name.startswith('__'):
            return result

        try:
            proxies = object.__getattribute__(self, '__sx_attrproxies__')
        except AttributeError:
            proxies = {}
            object.__setattr__(self, '__sx_attrproxies__', proxies)

        try:
            proxy = proxies[name]
        except KeyError:
            if isinstance(result, type):
                proxy = _proxy_class(self, result)
            else:
                proxy = ObjectProxy.__sx_new__(result)

            if proxy is not result:
                # Only record actual proxies
                proxies[name] = proxy

        result = proxy

        return result

    def __setattr__(self, name, value):
        if name in ('__name__', '__wrapped__'):
            object.__setattr__(self, name, value)

            if name == '__wrapped__':
                # __wrapped__ attribute is set by the loader when a module is (re-)loaded
                # When that happens, we need to make sure that all attribute proxies are
                # updated properly.
                try:
                    proxies = object.__getattribute__(self, '__sx_attrproxies__')
                except AttributeError:
                    pass
                else:
                    for attr, proxy in list(proxies.items()):
                        try:
                            attrval = getattr(value, attr)
                        except AttributeError:
                            del proxies[attr]
                        else:
                            proxy.__wrapped__ = attrval
        else:
            if not name.startswith('__'):
                # Create and update proxies when a module attribute is set.
                #
                try:
                    proxies = object.__getattribute__(self, '__sx_attrproxies__')
                except AttributeError:
                    proxies = {}
                    object.__setattr__(self, '__sx_attrproxies__', proxies)

                try:
                    proxy = proxies[name]
                except KeyError:
                    if isinstance(value, type):
                        proxy = _proxy_class(self, value)
                    else:
                        proxy = ObjectProxy.__sx_new__(value)
                else:
                    proxy.__wrapped__ = value

            super().__setattr__(name, value)

    def __delattr__(self, name):
        if name in ('__name__', '__wrapped__'):
            object.__delattr__(self, name)
        else:
            delattr(object.__getattribute__(self, '__wrapped__'), name)

            try:
                proxies = object.__getattribute__(self, '__sx_attrproxies__')
            except AttributeError:
                pass
            else:
                proxies.pop(name, None)


class ModuleInfo:
    def __init__(self, module=None, *, name=None, package=None, path=None, file=None):
        if module is not None:
            for attr in ('__name__', '__package__', '__path__', '__file__', '__spec__'):
                try:
                    v = getattr(module, attr)
                except AttributeError:
                    pass
                else:
                    setattr(self, attr, v)
        else:
            self.__name__ = name
            if package is not None:
                self.__package__ = package
            if path is not None:
                self.__path__ = path
            if file is not None:
                self.__file__ = file
