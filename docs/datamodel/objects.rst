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
value.  Object's identity is assigned on object's creation and never
changes.  Referring to object's ``id`` property yields its identity as a
:eql:type:`uuid` value.  Once set, the value of the ``id`` property
cannot be changed or masked with a different :ref:`computable
<ref_datamodel_computables>` expression.

Object types can *extend* other object types, in which case the
extending type is called a *subtype* and types being extended are
called *supertypes*. A subtype inherits all links, properties and
other aspects of its supertypes. It is possible to :ref:`extend more
than one type <ref_eql_sdl_object_types_inheritance>` - that's called
*multiple inheritance*. This mechanism allows building complex object
types out of combinations of more basic types.

``std::Object`` is the root of the object type hierarchy and all object
types in EdgeDB extend ``std::Object`` directly or indirectly.

.. eql:type:: std::Object

    Root object type.

    Definition:

    .. code-block:: sdl

        abstract type Object {
            # Universally unique object identifier
            required readonly property id -> uuid;

            # Object type in the information schema.
            required readonly link __type__ -> schema::ObjectType;
        }


See Also
--------

Object type
:ref:`SDL <ref_eql_sdl_object_types>`,
:ref:`DDL <ref_eql_ddl_object_types>`,
and :ref:`introspection <ref_eql_introspection_object_types>`.
