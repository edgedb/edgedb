.. _ref_datamodel_props:

==========
Properties
==========

:index: property

:ref:`Object types <ref_datamodel_object_types>` and
:ref:`links <ref_datamodel_links>` can contain *properties*: a name-value
collection of primitive data associated with the given object or link
instance.

Every property is declared to have a specific
:ref:`scalar type <ref_datamodel_scalar_types>` or a
:ref:`collection type <ref_datamodel_collection_types>` based on a scalar.

There are two kinds of property item declarations: *abstract properties*,
and *concrete properties*.  Abstract properties are defined on the module
level and are not tied to any particular object type or link.  Typically
this is done to set some :ref:`annotations <ref_datamodel_annotations>`,
or define :ref:`constraints <ref_datamodel_constraints>`.  Concrete
properties are defined on specific object types.

Similar to :ref:`links <ref_datamodel_links>`, properties have a
*source* (the object type or link on which they are defined) and one
or more *targets* (the values that property can have).


Object properties
-----------------

Properties defined on object types have the number of targets
specified by the keywords :ref:`required <ref_eql_ddl_props_syntax>`,
:ref:`single <ref_eql_ddl_props_syntax>`, and :ref:`multi
<ref_eql_ddl_props_syntax>`.  It is also possible to restrict how many
source objects can have the same property value via the
:eql:constraint:`exclusive` constraint.  For the purpose of figuring
out the number of property targets, a :ref:`collection type
<ref_datamodel_collection_types>` target by itself is considered a
*single* target.

For example, here's an object type with a *single required exclusive*
property ``name`` and an *optional multi* property ``favorite_tags``:

.. code-block:: sdl

    type Person {
        required property name -> str {
            constraint exclusive;
        }
        multi property favorite_tags -> str;
    }

.. note::

    Since the empty string ``''`` is a *value*, required properties can
    take on ``''`` as their value.


See Also
--------

Propery
:ref:`SDL <ref_eql_sdl_props>`,
:ref:`DDL <ref_eql_ddl_props>`,
and :ref:`introspection <ref_eql_introspection_object_types>`
(as part of overall object introspection).
