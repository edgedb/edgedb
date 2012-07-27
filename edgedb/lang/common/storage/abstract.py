##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import weakref

from semantix.utils import abc, slots


class BucketMeta(abc.AbstractMeta):
    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)
        if len([base for base in bases if isinstance(base, mcls)]) > 1:
            raise TypeError('Bucket classes can have only one base Bucket class')
        cls._instances = weakref.WeakSet()
        cls._providers = None
        return cls

    def _register_instance(cls, instance):
        cls._instances.add(instance)

    def set_providers(cls, *providers):
        cls._providers = providers

        impl = cls.get_implementation()

        for p in providers:
            if not isinstance(p, impl.compatible_provider_classes):
                raise TypeError('provider {!r} is not compatible with installed implementation '
                                '{!r}, must be an instance of {!r}'.
                                format(p, impl, impl.compatible_provider_classes))

    def get_providers(cls):
        if cls._providers is None:
            for parent in cls.__mro__[1:]:
                if not isinstance(parent, BucketMeta):
                    continue

                providers = parent._providers

                if providers is not None:
                    cls._providers = providers
                    return providers

            return
        else:
            return cls._providers

    def set_implementation(cls, implementation):
        if not issubclass(implementation, Implementation):
            raise ValueError('a subclass of Implementation was expected')

        if hasattr(cls, '_implementation') and '_implementation' not in cls.__dict__:
            holder = None
            for sub in cls.__mro__[1:-1]:
                if '_implementation' in sub.__dict__:
                    holder = sub
                    break

            raise ValueError('implementation was already defined in one of '
                             'the parent buckets: {!r}'.format(holder))

        cls._implementation = implementation

    def get_implementation(cls):
        return cls._implementation


class Bucket(metaclass=BucketMeta):
    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)
        cls._register_instance(instance)
        instance._providers = None
        instance._cached_implementation = None
        return instance

    def __init__(self, *, parent=None):
        if parent is not None:
            cls = type(self)
            mro = cls.__mro__[:-2] # Skip 'object' and 'abstract.Bucket'

            if type(parent) not in mro:
                raise ValueError('parent bucket {!r} must be an instance of one of the '
                                 'ancestor classes {!r}'.format(parent, mro))

            parent._register_child(self)

        self._parent = parent
        self._children = []

    def _register_child(self, bucket):
        self._children.append(bucket)

    def _get_implementation(self):
        if self._cached_implementation is None:
            self._providers = type(self).get_providers()
            if not self._providers:
                return

            self._cached_implementation = type(self).get_implementation()(self._providers)

        return self._cached_implementation

    def _ensure_implementation(self):
        impl = self._get_implementation()
        if not impl:
            raise KeyError('non-initialized bucket: no providers/implementation set')
        return impl


class ImplementationMeta(abc.AbstractMeta, slots.SlotsMeta):
    pass


class Implementation(metaclass=ImplementationMeta):
    compatible_provider_classes = None

    __slots__ = ('_providers',)

    def __init__(self, providers):
        self._providers = providers
