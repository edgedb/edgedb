##
# Copyright (c) 2009-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections

from edgedb.lang.common.functional import hybridmethod
from .marker import Void


class NoDefault(Void):
    pass


class Field:
    """``Field`` objects: attributes of :class:`Struct`."""

    __name__ = ('name', 'type', 'default', 'coerce', 'formatters')

    def __init__(
            self, type, default=NoDefault, *, coerce=False, str_formatter=str,
            repr_formatter=repr):
        """
        :param type: A type, or a tuple of types allowed for the field value.
        :param default: Default field value.  If not specified, the field would
                        be considered required and a failure to specify its
                        value when initializing a ``Struct`` will raise
                        :exc:`TypeError`.  `default` can be a callable taking
                        no arguments.
        :param bool coerce: If set to ``True`` - coerce field's value to its
                            type.
        """
        if not isinstance(type, tuple):
            type = (type, )

        self.type = type
        self.default = default
        self.coerce = coerce

        if coerce and len(type) > 1:
            raise ValueError(
                'unable to coerce values for fields with multiple types')

        self.formatters = {'str': str_formatter, 'repr': repr_formatter}

    def copy(self):
        return self.__class__(
            self.type, self.default, coerce=self.coerce,
            str_formatter=self.formatters['str'],
            repr_formatter=self.formatters['repr'])

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

    def __get__(self, instance, owner):
        if instance is not None:
            return None
        else:
            return self


class StructMeta(type):
    def __new__(mcls, name, bases, clsdict, *, use_slots=True):
        fields = {}
        myfields = {}

        for k, v in clsdict.items():
            if isinstance(v, Field):
                v.name = k
                myfields[k] = v

        if '__slots__' not in clsdict:
            if use_slots is None:
                for base in bases:
                    sa = '{}.{}_slots'.format(base.__module__, base.__name__)
                    if isinstance(base, StructMeta) and hasattr(base, sa):
                        use_slots = True
                        break

            if use_slots:
                clsdict['__slots__'] = tuple(myfields.keys())
                for key in myfields.keys():
                    del clsdict[key]

        cls = super().__new__(mcls, name, bases, clsdict)

        if use_slots:
            sa = '{}.{}_slots'.format(cls.__module__, cls.__name__)
            setattr(cls, sa, True)

        for parent in reversed(cls.__mro__):
            if parent is cls:
                fields.update(myfields)
            elif isinstance(parent, StructMeta):
                fields.update(parent.get_ownfields())

        cls._fields = fields
        cls._sorted_fields = collections.OrderedDict(
            sorted(fields.items(), key=lambda e: e[0]))
        fa = '{}.{}_fields'.format(cls.__module__, cls.__name__)
        setattr(cls, fa, myfields)
        return cls

    def __init__(cls, name, bases, clsdict, *, use_slots=None):
        super().__init__(name, bases, clsdict)

    def get_field(cls, name):
        return cls._fields[name]

    def get_fields(cls, sorted=False):
        return cls._sorted_fields if sorted else cls._fields

    def get_ownfields(cls):
        return getattr(
            cls, '{}.{}_fields'.format(cls.__module__, cls.__name__))


class Struct(metaclass=StructMeta):
    """A base class allowing implementation of attribute objects protocols.

    Each struct has a collection of ``Field`` objects, which should be defined
    as class attributes of the ``Struct`` subclass.  Unlike
    ``collections.namedtuple``, ``Struct`` is much easier to mix in and define.
    Furthermore, fields are strictly typed and can be declared as required.  By
    default, Struct will reject attributes, which have not been declared as
    fields.  A ``MixedStruct`` subclass does have this restriction.

    .. code-block:: pycon

        >>> from edgedb.lang.common.datastructures import Struct, Field

        >>> class MyStruct(Struct):
        ...    name = Field(type=str)
        ...    description = Field(type=str, default=None)
        ...
        >>> MyStruct(name='Spam')
        <MyStruct name=Spam>
        >>> MyStruct(name='Ham', description='Good Ham')
        <MyStruct name=Ham, description=Good Ham>

    If ``use_slots`` is set to ``True`` in a class signature, ``__slots__``
    will be used to create dictless instances, with reduced memory footprint:

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
        :param bool _setdefaults_: If False, fields will not be initialized
                                   with default values immediately.  It is
                                   possible to call ``Struct.setdefaults()``
                                   later to initialize unset fields.

        :param bool _relaxrequired_: If True, missing values for required
                                     fields will not
                                     cause an exception.

        :raises: TypeError if invalid field value was provided or a value was
                 not provided for a field without a default value and
                 `_relaxrequired_` is False.
        """
        self._check_init_argnames(kwargs)
        self._init_fields(_setdefaults_, _relaxrequired_, kwargs)

    def __setstate__(self, state):
        if isinstance(state, tuple) and len(state) == 2:
            state, slotstate = state
        else:
            slotstate = None

        if state:
            self.update(**state)

        if slotstate:
            self.update(**slotstate)

    def update(self, *args, **kwargs):
        """Update the field values."""
        values = {}
        values.update(*args, **kwargs)

        self._check_init_argnames(values)

        for k, v in values.items():
            setattr(self, k, v)

    def setdefaults(self):
        """Initialize unset fields with default values."""
        fields_set = []
        for field_name, field in self.__class__._fields.items():
            value = getattr(self, field_name)
            if value is None and field.default is not None:
                value = self._getdefault(field_name, field)
                self.set_default_value(field_name, value)
                fields_set.append(field_name)
        return fields_set

    def set_default_value(self, field_name, value):
        setattr(self, field_name, value)

    def formatfields(self, formatter='str'):
        """Return an iterator over fields formatted using `formatter`."""
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

    def items(self):
        for field in self.__class__._fields:
            yield field, getattr(self, field, None)

    __copy__ = copy

    def __iter__(self):
        return iter(self.__class__._fields)

    def __str__(self):
        fields = ', '.join(('%s=%s' % (name, value))
                           for name, value in self.formatfields('str'))
        return '<{}{}>'.format(
            self.__class__.__name__, ' ' + fields if fields else '')

    def __repr__(self):
        fields = ', '.join(('%s=%s' % (name, value))
                           for name, value in self.formatfields('repr'))
        return '<{}{}>'.format(
            self.__class__.__name__, ' ' + fields if fields else '')

    def _init_fields(self, setdefaults, relaxrequired, values):
        for field_name, field in self.__class__._fields.items():
            value = values.get(field_name)

            if value is None and field.default is not None and setdefaults:
                value = self._getdefault(field_name, field, relaxrequired)

            setattr(self, field_name, value)

    def __setattr__(self, name, value):
        field = self._fields.get(name)
        if field is not None:
            value = self._check_field_type(field, name, value)
        super().__setattr__(name, value)

    def _check_init_argnames(self, args):
        extra = set(args) - set(self.__class__._fields)
        if extra:
            fmt = '{} {} invalid argument{} for struct {}.{}'
            plural = len(extra) > 1
            msg = fmt.format(
                ', '.join(extra), 'are' if plural else 'is an', 's' if plural
                else '', self.__class__.__module__, self.__class__.__name__)
            raise TypeError(msg)

    def _check_field_type(self, field, name, value):
        if (
                field.type and value is not None and value is not Void and
                not isinstance(value, field.type)):
            if field.coerce:
                try:
                    return field.type[0](value)
                except Exception as ex:
                    raise TypeError(
                        'cannot coerce {!r} value {!r} '
                        'to {}'.format(name, value, field.type)) from ex

            raise TypeError(
                '{}.{}.{}: expected {} but got {!r}'.format(
                    self.__class__.__module__, self.__class__.__name__, name,
                    ' or '.join(t.__name__ for t in field.type), value))

        return value

    def _getdefault(self, field_name, field, relaxrequired=False):
        if field.default in field.type:
            value = field.default()
        elif field.default is NoDefault:
            if relaxrequired:
                value = None
            else:
                raise TypeError(
                    '%s.%s.%s is required' % (
                        self.__class__.__module__, self.__class__.__name__,
                        field_name))
        else:
            value = field.default
        return value

    def get_field_value(self, field_name):
        try:
            return self.__dict__[field_name]
        except KeyError as e:
            field = self.__class__.get_field(field_name)
            try:
                return self._getdefault(field_name, field)
            except TypeError:
                raise e


class MixedStructMeta(StructMeta):
    def __new__(mcls, name, bases, clsdict, *, use_slots=False):
        return super().__new__(mcls, name, bases, clsdict, use_slots=use_slots)


class MixedStruct(Struct, metaclass=MixedStructMeta):
    def _check_init_argnames(self, args):
        pass
