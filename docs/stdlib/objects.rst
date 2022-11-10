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


:eql:type:`BaseObject` is the root of the object type hierarchy and all
object types in EdgeDB, including system types, whether extending it directly
or indirectly. User-defined object types extend from the :eql:type:`Object`
type, which is a subtype of ``std::BaseObject``.


---------


.. eql:type:: std::BaseObject

    Represents the root object of a type:

    .. code-block:: sdl

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

    Some subtypes may override the ``id`` property, but only to validate
    UUID generation functions. Currently, these include
    :eql:func:`uuid_generate_v1mc` and :eql:func:`uuid_generate_v4`.


---------


.. eql:type:: std::Object

    Represents the root object of a type for all user-defined types:

    .. code-block:: sdl

        abstract type std::Object extending std::BaseObject;
