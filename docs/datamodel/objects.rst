.. _ref_datamodel_object_types:

============
Object Types
============

Object types are the primary components of EdgeDB schema.  An object type
is a collection of named :ref:`properties <ref_datamodel_props>` and
:ref:`links <ref_datamodel_links>` to other object types.   An instance of
an object type is called an *object*.  All data in EdgeDB is represented by
objects and by links between objects.

Every object has a globally unique *identity* represented by a ``UUID``
value. An object's identity is assigned upon creation and never changes.
Referring to its id property yields its identity as a
:eql:type:`uuid` value.  Once set, the value of the ``id`` property
cannot be changed or masked with a different :ref:`computed
<ref_datamodel_computables>` expression.

Object types can *extend* other object types, in which case the
extending type is called a *subtype* and types being extended are
called *supertypes*. A subtype inherits all links, properties and
other aspects of its supertypes. It is possible to :ref:`extend more
than one type <ref_eql_sdl_object_types_inheritance>` - that's called
*multiple inheritance*. This mechanism allows building complex object
types out of combinations of more basic types.

``std::BaseObject`` is the root of the object type hierarchy and all
object types in EdgeDB, including system types, extend ``std::BaseObject``
directly or indirectly.  User-defined object types extend from ``std::Object``,
which is a subtype of ``std::BaseObject``.

.. eql:type:: std::BaseObject

    Root object type.

    Definition:

    .. code-block:: sdl

        abstract type std::BaseObject {
            # Universally unique object identifier
            required readonly property id -> uuid;

            # Object type in the information schema.
            required readonly link __type__ -> schema::ObjectType;
        }

.. eql:type:: std::Object

    Root object type for user-defined types.

    Definition:

    .. code-block:: sdl

        abstract type std::Object extending std::BaseObject;


.. _ref_datamodel_object_types_free:

Free Objects
============

It is also possible to package data into a *free object*.
*Free objects* are meant to be transient and used either to more
efficiently store some intermediate results in a query or for
re-shaping the output. The advantage of using *free objects* over
:eql:type:`tuples <tuple>` is that it is easier to package data that
potentially contains empty sets as links or properties of the
*free object*.

Consider the following query:

.. code-block:: edgeql

    WITH U := (SELECT User FILTER .name LIKE '%user%')
    SELECT {
        matches := U {name},
        total := count(U),
        total_users := count(User),
    };

The ``matches`` are potentially ``{}``, yet the query will always
return a single *free object* with ``resutls``, ``total``, and
``total_users``. To achieve the same using a :eql:type:`named tuple
<tuple>`, the query would have to be modified like this:

.. code-block:: edgeql

    WITH U := (SELECT User FILTER .name LIKE '%user%')
    SELECT (
        matches := array_agg(U {name}),
        total := count(U),
        total_users := count(User),
    );

Without the :eql:func:`array_agg` the above query would return ``{}``
instead of the named tuple if no ``matches`` are found.


See Also
--------

Object type
:ref:`SDL <ref_eql_sdl_object_types>`,
:ref:`DDL <ref_eql_ddl_object_types>`,
and :ref:`introspection <ref_eql_introspection_object_types>`.
