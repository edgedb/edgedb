.. _ref_datamodel_linkprops:

===============
Link Properties
===============

:index: property

Properties defined on links are a little more restricted than the ones
defined on object types. They can never be :ref:`required
<ref_eql_ddl_props_syntax>`, so that a link can exist just fine
without them. They also have to be :ref:`single
<ref_eql_ddl_props_syntax>`.

Typically link properties are used to indicate some flavor or strength
of a particular relationship, such as ordering or total count:

.. code-block:: sdl

    type Person {
        required property name -> str {
            constraint exclusive;
        }
        multi link shirts -> Shirt {
            constraint exclusive;
            # This is a good way of keeping track of
            # identical Shirts, since creating identical
            # Shirts would violate the exclusivity
            # constraint of the description.
            property count -> int64;
        }
    }
    type Shirt {
        required property description -> str {
            constraint exclusive;
        }
    }


See Also
--------

Propery
:ref:`SDL <ref_eql_sdl_props>`,
:ref:`DDL <ref_eql_ddl_props>`,
and :ref:`introspection <ref_eql_introspection_object_types>`
(as part of overall object introspection).
