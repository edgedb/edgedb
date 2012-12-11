##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""A limited implementation of Multiple Dispatch pattern."""


from metamagic.utils.datastructures.registry import WeakObjectRegistry


_TYPE_HANDLER = 1
_TYPE_METHOD_NAME = 2


class _TypeDispatcherMeta(type):
    """Metaclass for ``Dispatcher`` metaclass.  Adds own 'registry' attribute to each
    ``Dispatcher`` metaclass, so each ``Dispatcher`` has it's own registry.
    """

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        # We don't want to store hard references to classes, since the
        # ``_registry`` serves as a cache too, hence we use ``WeakObjectRegistry``.
        #
        cls._registry = WeakObjectRegistry()
        return cls

    def __call__(cls, *, handles=None, method=None):
        """Acts as a decorator.  Only one parameter should be specified.

        :param type handles: If a function is decorated, ``handles`` specifies
                             class of objectes that can be handled by it.
        :param string method: If a class is decorated, ``method`` specifies
                              name of a ``@classmethod`` or ``@staticmethod``
                              that handles the class (and its derivatives)
        """

        if handles is None and method is None:
            raise TypeError('either `handles` or `method` arguments should be passed')

        if handles is not None and method is not None:
            raise TypeError('both `handles` and `method` arguments passed')

        if handles is not None:
            # We're decorating a function
            #
            if not isinstance(handles, tuple):
                handles = (handles,)

            def wrap(handler, cls=cls, handles=handles):
                for t in handles:
                    if not isinstance(t, type):
                        raise ValueError('Dispatcher handles only classes, got {!r}'.format(t))

                    cls._registry[t] = (_TYPE_HANDLER, handler)
                return handler

        if method is not None:
            # We're decorating a class
            #
            def wrap(decorated, cls=cls, method_name=method):
                if not isinstance(decorated, type):
                    raise TypeError('a class was expected to be decorated, got {!r}'. \
                                    format(decorated))

                try:
                    getattr(decorated, method_name)
                except AttributeError:
                    raise TypeError('no method {!r} is found in class {!r}'. \
                                    format(method_name, decorated))

                cls._registry[decorated] = (_TYPE_METHOD_NAME, method_name)
                return decorated

        return wrap

    def _unpack_handler(cls, type, descriptor):
        if descriptor[0] == _TYPE_HANDLER:
            return descriptor[1]
        else:
            assert descriptor[0] == _TYPE_METHOD_NAME
            return getattr(type, descriptor[1])

    def get_handler(cls, type):
        """Looks for the most suitable ``handler`` for the given class.

        :param type type: A class to look the handler for.
        :raises LookupError: If no handler found.
        """

        # Already have a handler for ``type`` in registry?
        #
        try:
            descriptor =  cls._registry[type]
        except KeyError:
            pass
        else:
            return cls._unpack_handler(type, descriptor)

        descriptor = None
        previous_descriptors = set()

        # The algorithm: Iterate through ``type``'s MRO in reversed order.
        # For each class in MRO try to find appropriate handler in the
        # ``Dispatcher._registry``.  If a handler was found, and was never
        # encountered before, store it to the ``previous_handlers`` set
        # and remember it as a candidate for the final handler.
        #
        # By traversing MRO in the reversed order, we should ensure that
        # we find the handler registered for the class closer to the beginning
        # of ``type``'s MRO, hence, more suitable for the ``type``.
        #
        for base in reversed(type.__mro__):
            for candidate_type, candidate_descriptor in cls._registry.items():
                if issubclass(base, candidate_type):
                    if candidate_descriptor in previous_descriptors:
                        continue
                    descriptor = candidate_descriptor
                    previous_descriptors.add(candidate_descriptor)

        if descriptor is None:
            raise LookupError('unable to find handler for {!r}'.format(cls))

        # Store the handler in the ``_registry`` for the ``type``, so next time
        # we won't need any time-expensive computations.
        #
        cls._registry[type] = descriptor
        return cls._unpack_handler(type, descriptor)


class TypeDispatcher(metaclass=_TypeDispatcherMeta):
    """This class is a base class for your dispatchers. **Don't** use it directly,
    just derive a class from it.

    Usage example:

    .. code-block:: python

        class renderer(TypeDispatcher):
            pass

        @renderer(handles=(int, float))
        def render_int(obj):
            '''Renders int or float objects'''
            return 'int: {}'.format(obj)

        @renderer(handles=str)
        def render_str(obj):
            return 'str: {}'.format(obj)

        @renderer(method='render')
        class Foo:
            @classmethod
            def render(cls, obj):
                return 'Foo'

        class Bar(Foo):
            @staticmethod
            def render(obj):
                return 'Bar'

        def render(obj):
            method = renderer.get_handler(type(obj))
            return method(obj)

    .. code-block:: pycon

        >>> render(123)
        int: 123
        >>> render('abc')
        str: abc
        >>> render(Foo())
        Foo
        >>> render(Bar())
        Bar

    .. note:: This class cannot be instantiated, and should be used as a decorator only.
    """

    def __init__(self):
        raise TypeError('{!r} dispatcher is not supposed to be instantiated')
