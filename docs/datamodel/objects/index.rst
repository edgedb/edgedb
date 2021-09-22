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
<ref_datamodel_computables>` expression. The globally unique
``id`` guarantees that the database does not contain any object
duplicates.

Object types can *extend* other object types, in which case the
extending type is called a *subtype* and types being extended are
called *supertypes*. A subtype inherits all links, properties and
other aspects of its supertypes. It is possible to :ref:`extend more
than one type <ref_eql_sdl_object_types_inheritance>` - that's called
*multiple inheritance*. This mechanism allows building complex object
types out of combinations of more basic types.

.. toctree::
    :maxdepth: 3
    :hidden:

    user
    free
