.. _ref_std_object_types:

============
Base Objects
============

.. list-table::
    :class: funcoptable

    * - :eql:type:`BaseObject`
      - Root object type

    * - :eql:type:`Object`
      - Root for user-defined object types


``std::BaseObject`` is the root of the object type hierarchy and all object
types in EdgeDB, including system types, extend it either directly or
indirectly.  User-defined object types extend from :eql:type:`std::Object`
type, which is a subtype of ``std::BaseObject``.


---------


.. eql:type:: std::BaseObject

    The root object type.

    Definition:

    .. code-block:: sdl
        :version-lt: 3.0

        abstract type std::BaseObject {
            # Universally unique object identifier
            required property id -> uuid {
                default := (select std::uuid_generate_v1mc());
                readonly := true;
                constraint exclusive;
            }

            # Object type in the information schema.
            required readonly link __type__ -> schema::ObjectType;
        }

    .. code-block:: sdl

        abstract type std::BaseObject {
            # Universally unique object identifier
            required id: uuid {
                default := (select std::uuid_generate_v1mc());
                readonly := true;
                constraint exclusive;
            }

            # Object type in the information schema.
            required readonly __type__: schema::ObjectType;
        }

    Subtypes may override the ``id`` property, but only with a valid UUID
    generation function. Currently, these are :eql:func:`uuid_generate_v1mc`
    and :eql:func:`uuid_generate_v4`.


---------


.. eql:type:: std::Object

    The root object type for user-defined types.

    Definition:

    .. code-block:: sdl

        abstract type std::Object extending std::BaseObject;
