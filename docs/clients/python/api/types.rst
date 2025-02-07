.. _edgedb-python-datatypes:

=========
Datatypes
=========

.. py:currentmodule:: edgedb


edgedb-python automatically converts EdgeDB types to the corresponding Python
types and vice versa.

The table below shows the correspondence between EdgeDB and Python types.

+----------------------------+-----------------------------------------------------+
| EdgeDB Type                |  Python Type                                        |
+============================+=====================================================+
| ``Set``                    | :py:class:`edgedb.Set`                              |
+----------------------------+-----------------------------------------------------+
| ``array<anytype>``         | :py:class:`edgedb.Array`                            |
+----------------------------+-----------------------------------------------------+
| ``anytuple``               | :py:class:`edgedb.Tuple` or                         |
|                            | :py:class:`edgedb.NamedTuple`                       |
+----------------------------+-----------------------------------------------------+
| ``anyenum``                | :py:class:`edgedb.EnumValue`                        |
+----------------------------+-----------------------------------------------------+
| ``Object``                 | :py:class:`edgedb.Object`                           |
+----------------------------+-----------------------------------------------------+
| ``bool``                   | :py:class:`bool <python:bool>`                      |
+----------------------------+-----------------------------------------------------+
| ``bytes``                  | :py:class:`bytes <python:bytes>`                    |
+----------------------------+-----------------------------------------------------+
| ``str``                    | :py:class:`str <python:str>`                        |
+----------------------------+-----------------------------------------------------+
| ``cal::local_date``        | :py:class:`datetime.date <python:datetime.date>`    |
+----------------------------+-----------------------------------------------------+
| ``cal::local_time``        | offset-naive :py:class:`datetime.time \             |
|                            | <python:datetime.time>`                             |
+----------------------------+-----------------------------------------------------+
| ``cal::local_datetime``    | offset-naive :py:class:`datetime.datetime \         |
|                            | <python:datetime.datetime>`                         |
+----------------------------+-----------------------------------------------------+
| ``cal::relative_duration`` | :py:class:`edgedb.RelativeDuration`                 |
+----------------------------+-----------------------------------------------------+
| ``cal::date_duration``     | :py:class:`edgedb.DateDuration`                     |
+----------------------------+-----------------------------------------------------+
| ``datetime``               | offset-aware :py:class:`datetime.datetime \         |
|                            | <python:datetime.datetime>`                         |
+----------------------------+-----------------------------------------------------+
| ``duration``               | :py:class:`datetime.timedelta \                     |
|                            | <python:datetime.timedelta>`                        |
+----------------------------+-----------------------------------------------------+
| ``float32``,               | :py:class:`float <python:float>`                    |
| ``float64``                |                                                     |
+----------------------------+-----------------------------------------------------+
| ``int16``,                 | :py:class:`int <python:int>`                        |
| ``int32``,                 |                                                     |
| ``int64``,                 |                                                     |
| ``bigint``                 |                                                     |
+----------------------------+-----------------------------------------------------+
| ``decimal``                | :py:class:`Decimal <python:decimal.Decimal>`        |
+----------------------------+-----------------------------------------------------+
| ``json``                   | :py:class:`str <python:str>`                        |
+----------------------------+-----------------------------------------------------+
| ``uuid``                   | :py:class:`uuid.UUID <python:uuid.UUID>`            |
+----------------------------+-----------------------------------------------------+

.. note::

    Inexact single-precision ``float`` values may have a different
    representation when decoded into a Python float.  This is inherent
    to the implementation of limited-precision floating point types.
    If you need the decimal representation to match, cast the expression
    to ``float64`` or ``decimal`` in your query.


.. _edgedb-python-types-set:

Sets
====

.. py:class:: Set()

    This is :py:class:`list <python:list>` since version 1.0.


.. _edgedb-python-types-object:

Objects
=======

.. py:class:: Object()

    An immutable representation of an object instance returned from a query.

    .. versionchanged:: 1.0

        ``edgedb.Object`` instances are dataclass-compatible since version 1.0,
        for example, ``dataclasses.is_dataclass()`` will return ``True``, and
        ``dataclasses.asdict()`` will work on ``edgedb.Object`` instances.

    .. versionchanged:: 1.0

        ``edgedb.Object.__hash__`` is just ``object.__hash__`` in version 1.0.
        Similarly, ``==`` is equivalent to the ``is`` operator comparing
        ``edgedb.Object`` instances, and ``<``, ``<=``, ``>``, ``>=`` are not
        allowed on ``edgedb.Object`` instances.

    The value of an object property or a link can be accessed through
    a corresponding attribute:

    .. code-block:: pycon

        >>> import edgedb
        >>> client = edgedb.create_client()
        >>> r = client.query_single('''
        ...     SELECT schema::ObjectType {name}
        ...     FILTER .name = 'std::Object'
        ...     LIMIT 1''')
        >>> r
        Object{name := 'std::Object'}
        >>> r.name
        'std::Object'

    .. describe:: obj[linkname]

       Return a :py:class:`edgedb.Link` or a :py:class:`edgedb.LinkSet` instance
       representing the instance(s) of link *linkname* associated with
       *obj*.

       Example:

       .. code-block:: pycon

          >>> import edgedb
          >>> client = edgedb.create_client()
          >>> r = client.query_single('''
          ...     SELECT schema::Property {name, annotations: {name, @value}}
          ...     FILTER .name = 'listen_port'
          ...            AND .source.name = 'cfg::Config'
          ...     LIMIT 1''')
          >>> r
          Object {
              name: 'listen_port',
              annotations: {
                  Object {
                      name: 'cfg::system',
                      @value: 'true'
                  }
              }
          }
          >>> r['annotations']
          LinkSet(name='annotations')
          >>> l = list(r['annotations])[0]
          >>> l.value
          'true'


Links
=====

.. py:class:: Link

    An immutable representation of an object link.

    Links are created when :py:class:`edgedb.Object` is accessed via
    a ``[]`` operator.  Using Link objects explicitly is useful for
    accessing link properties.


.. py:class:: LinkSet

    An immutable representation of a set of Links.

    LinkSets are created when a multi link on :py:class:`edgedb.Object`
    is accessed via a ``[]`` operator.


Tuples
======

.. py:class:: Tuple()

    This is :py:class:`tuple <python:tuple>` since version 1.0.


Named Tuples
============

.. py:class:: NamedTuple()

    An immutable value representing an EdgeDB named tuple value.

    .. versionchanged:: 1.0

        ``edgedb.NamedTuple`` is a subclass of :py:class:`tuple <python:tuple>`
        and is duck-type compatible with ``collections.namedtuple`` since
        version 1.0.

    Instances of ``edgedb.NamedTuple`` generally behave similarly to
    :py:func:`namedtuple <python:collections.namedtuple>`:

    .. code-block:: pycon

        >>> import edgedb
        >>> client = edgedb.create_client()
        >>> r = client.query_single('''SELECT (a := 1, b := 'a', c := [3])''')
        >>> r
        (a := 1, b := 'a', c := [3])
        >>> r.b
        'a'
        >>> r[0]
        1
        >>> r == (1, 'a', [3])
        True
        >>> r._fields
        ('a', 'b', 'c')


Arrays
======

.. py:class:: Array()

    This is :py:class:`list <python:list>` since version 1.0.


RelativeDuration
================

.. py:class:: RelativeDuration()

    An immutable value representing an EdgeDB ``cal::relative_duration`` value.

    .. code-block:: pycon

        >>> import edgedb
        >>> client = edgedb.create_client()
        >>> r = client.query_single('''SELECT <cal::relative_duration>"1 year 2 days 3 seconds"''')
        >>> r
        <edgedb.RelativeDuration "P1Y2DT3S">
        >>> r.months
        12
        >>> r.days
        2
        >>> r.microseconds
        3000000


DateDuration
============

.. py:class:: DateDuration()

    An immutable value representing an EdgeDB ``cal::date_duration`` value.

    .. code-block:: pycon

        >>> import edgedb
        >>> client = edgedb.create_client()
        >>> r = client.query_single('''SELECT <cal::date_duration>"1 year 2 days"''')
        >>> r
        <edgedb.DateDuration "P1Y2D">
        >>> r.months
        12
        >>> r.days
        2


EnumValue
=========

.. py:class:: EnumValue()

    An immutable value representing an EdgeDB enum value.

    .. versionchanged:: 1.0

        Since version 1.0, ``edgedb.EnumValue`` is a subclass of
        :py:class:`enum.Enum <python:enum.Enum>`. Actual enum values are
        instances of ad-hoc enum classes created by the codecs to represent
        the actual members defined in your EdgeDB schema.

    .. code-block:: pycon

        >>> import edgedb
        >>> client = edgedb.create_client()
        >>> r = client.query_single("""SELECT <Color>'red'""")
        >>> r
        <edgedb.EnumValue 'red'>
        >>> str(r)
        'red'
        >>> r.value  # added in 1.0
        'red'
        >>> r.name  # added in 1.0, simply str.upper() of r.value
        'RED'
