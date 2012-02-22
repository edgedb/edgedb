##
# Copyright (c) 2009-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.functional import hybridmethod
from .all import Void


class Field:
    """``Field`` objects are meant to be specified as attributes of :class:`Struct`"""

    def __init__(self, type, default=Void, *, coerce=False,
                 str_formatter=str, repr_formatter=repr):
        """
        :param type: A type, or a tuple of types allowed for the field value.
        :param default: Default field value.  If not specified, the field would be considered
                        required and a failure to specify its value when initializing a ``Struct``
                        will raise :exc:`TypeError`.  `default` can be a callable taking no
                        arguments.
        :param bool coerce: If set to ``True`` - coerce field's value to its type.
        """

        if not isinstance(type, tuple):
            type = (type,)

        self.type = type
        self.default = default
        self.coerce = coerce

        if coerce and len(type) > 1:
            raise ValueError('unable to coerce values for fields with multiple types')

        self.formatters = {'str': str_formatter, 'repr': repr_formatter}

    def adapt(self, value):
        if not isinstance(value, self.type):
            for t in self.type:
                try:
                    value = t(value)
                except TypeError:
                    pass
                else:
                    break

        return value


class StructMeta(type):
    def __new__(mcls, name, bases, clsdict, *, use_slots=True):
        fields = {}
        myfields = {k: v for k, v in clsdict.items() if isinstance(v, Field)}

        if '__slots__' not in clsdict:
            if use_slots is None:
                for base in bases:
                    if isinstance(base, StructMeta) and \
                            hasattr(base, '{}.{}_slots'.format(base.__module__, base.__name__)):

                        use_slots = True
                        break

            if use_slots:
                clsdict['__slots__'] = tuple(myfields.keys())
                for key in myfields.keys():
                    del clsdict[key]

        cls = super().__new__(mcls, name, bases, clsdict)

        if use_slots:
            setattr(cls, '{}.{}_slots'.format(cls.__module__, cls.__name__), True)

        for parent in reversed(cls.mro()):
            if parent is cls:
                fields.update(myfields)
            elif isinstance(parent, StructMeta):
                fields.update(parent.get_ownfields())

        cls._fields = fields
        setattr(cls, '{}.{}_fields'.format(cls.__module__, cls.__name__), myfields)
        return cls

    def __init__(cls, name, bases, clsdict, *, use_slots=None):
        super().__init__(name, bases, clsdict)

    def get_fields(cls):
        return cls._fields

    def get_ownfields(cls):
        return getattr(cls, '{}.{}_fields'.format(cls.__module__, cls.__name__))


class Struct(metaclass=StructMeta):
    """Struct classes provide a way to define, maintain and introspect strict data structures.

    Each struct has a collection of ``Field`` objects, which should be defined as class
    attributes of the ``Struct`` subclass.  Unlike ``collections.namedtuple``, ``Struct`` is
    much easier to mix in and define.  Furthermore, fields are strictly typed and can be
    declared as required.  By default, Struct will reject attributes, which have not been
    declared as fields.  A ``MixedStruct`` subclass does have this restriction.

    .. code-block:: pycon

        >>> from semantix.utils.datastructures import Struct, Field

        >>> class MyStruct(Struct):
        ...    name = Field(type=str)
        ...    description = Field(type=str, default=None)
        ...
        >>> MyStruct(name='Spam')
        <MyStruct name=Spam>
        >>> MyStruct(name='Ham', description='Good Ham')
        <MyStruct name=Ham, description=Good Ham>

    If ``use_slots`` is set to ``True`` in a class signature, ``__slots__`` will be used
    to create dictless instances, with reduced memory footprint:

    .. code-block:: pycon

        >>> class S1(Struct, use_slots=True):
        ...     foo = Field(str, None)

        >>> class S2(S1):
        ...     bar = Field(str, None)

        >>> S2().foo = '1'
        >>> S2().bar = '2'

        >>> S2().spam = '2'
        AttributeError: 'S2' object has no attribute 'spam'
    """

    __slots__ = ()

    def __init__(self, *, _setdefaults_=True, _relaxrequired_=False, **kwargs):
        """
        :param bool _setdefaults_: If False, fields will not be initialized with default
                                   values immediately.  It is possible to call
                                   ``Struct.setdefaults()`` later to initialize unset fields.

        :param bool _relaxrequired_: If True, missing values for required fields will not
                                     cause an exception.

        :raises: TypeError if invalid field value was provided or a value was not provided
                 for a field without a default value and `_relaxrequired_` is False.
        """
        self._check_init_argnames(kwargs)
        self._init_fields(_setdefaults_, _relaxrequired_, kwargs)

    def update(self, *args, **kwargs):
        """Updates the field values from dict/iterable and `**kwargs` similarly to :py:meth:`dict.update()`"""

        values = {}
        values.update(values, *args, **kwargs)

        self._check_init_argnames(values)

        for k, v in values.items():
            setattr(self, k, v)

    def setdefaults(self):
        """Initializes unset fields with default values.  Useful for deferred initialization"""

        for field_name, field  in self.__class__._fields.items():
            value = getattr(self, field_name)
            if value is None and field.default is not None:
                value = self._getdefault(field_name, field)
                setattr(self, field_name, value)

    def formatfields(self, formatter='str'):
        """Returns an iterator over fields formatted using `formatter`"""

        for name, field in self.__class__._fields.items():
            formatter_obj = field.formatters.get(formatter)
            if formatter_obj:
                yield (name, formatter_obj(getattr(self, name)))

    @hybridmethod
    def copy(scope, obj=None):
        if isinstance(scope, Struct):
            obj = scope
            cls = obj.__class__
        else:
            cls = scope

        args = {f: getattr(obj, f) for f in cls._fields.keys()}
        return cls(**args)

    __copy__ = copy

    def __iter__(self):
        return iter(self.__class__._fields)

    def __str__(self):
        fields = ', '.join(('%s=%s' % (name, value)) for name, value in self.formatfields('str'))
        return '<{} {}>'.format(self.__class__.__name__, fields)

    def __repr__(self):
        fields = ', '.join(('%s=%s' % (name, value)) for name, value in self.formatfields('repr'))
        return '<{} {}>'.format(self.__class__.__name__, fields)

    # XXX: the following is a CC from AST, consider consolidation
    def _init_fields(self, setdefaults, relaxrequired, values):
        for field_name, field  in self.__class__._fields.items():
            value = values.get(field_name)

            if value is None and field.default is not None and setdefaults:
                value = self._getdefault(field_name, field, relaxrequired)

            setattr(self, field_name, value)

    if __debug__:
        def __setattr__(self, name, value):
            field = self._fields.get(name)
            if field:
                value = self._check_field_type(field, name, value)
            super().__setattr__(name, value)

    def _check_init_argnames(self, args):
        extra = set(args) - set(self.__class__._fields)
        if extra:
            fmt = '{} {} invalid argument{} for struct {}.{}'
            plural = len(extra) > 1
            msg = fmt.format(', '.join(extra), 'are' if plural else 'is an', 's' if plural else '',
                             self.__class__.__module__, self.__class__.__name__)
            raise TypeError(msg)

    def _check_field_type(self, field, name, value):
        if field.type and value is not None and not isinstance(value, field.type):
            if field.coerce:
                try:
                    return field.type[0](value)
                except Exception as ex:
                    raise TypeError('exception during field {!r} value {!r} auto-coercion to {}'. \
                                    format(name, value, field.type)) from ex

            raise TypeError('{}.{}.{}: expected {} but got {!r}'. \
                            format(self.__class__.__module__,
                                   self.__class__.__name__,
                                   name,
                                   ' or '.join(t.__name__ for t in field.type),
                                   value))

        return value

    def _getdefault(self, field_name, field, relaxrequired=False):
        if field.default in field.type:
            value = field.default()
        elif field.default is Void:
            if relaxrequired:
                value = None
            else:
                raise TypeError('%s.%s.%s is required' % (self.__class__.__module__,
                                                          self.__class__.__name__,
                                                          field_name))
        else:
            value = field.default
        return value


class MixedStructMeta(StructMeta):
    def __new__(mcls, name, bases, clsdict, *, use_slots=False):
        return super().__new__(mcls, name, bases, clsdict, use_slots=use_slots)


class MixedStruct(Struct, metaclass=MixedStructMeta):
    def _check_init_argnames(self, args):
        pass
