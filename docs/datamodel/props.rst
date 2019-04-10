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
and *concrete properties*.  Abstract properties are defined on module level
and are not tied to any particular object type or link.  Typically this is
done to set some :ref:`annotations <ref_datamodel_annotations>`, or define
:ref:`constraints <ref_datamodel_constraints>`.  Concrete properties
are defined on specific object types.

Similarly to :ref:`links <ref_datamodel_links>` properties have a
*source* (the object on which they are defined) and one or more
*targets* (the values that property can have). The number of targets
as specified by keywords :ref:`required <ref_eql_ddl_props_syntax>`,
:ref:`single <ref_eql_ddl_props_syntax>`, and :ref:`multi
<ref_eql_ddl_props_syntax>`.  It is also possible to restrict how many
source objects can link to the same target via
:eql:constraint:`exclusive` constraint.  For the purpose of figuring
out the number of property targets, a :ref:`collection type
<ref_datamodel_collection_types>` target by itself is considered a
*single* target.


See Also
--------

Propery
:ref:`SDL <ref_eql_sdl_props>`,
:ref:`DDL <ref_eql_ddl_props>`,
and :ref:`introspection <ref_eql_introspection_object_types>`
(as part of overall object introspection).
