.. _ref_datamodel_props:

==========
Properties
==========

:ref:`Object types <ref_datamodel_object_types>` and
:ref:`links <ref_datamodel_links>` can contain *properties*: a name-value
collection of primitive data associated with the given object or link
instance.

Every property is declared to have a specific
:ref:`scalar type <ref_datamodel_scalar_types>` or a
:ref:`collection type <ref_datamodel_collection_types>`.

There are two kinds of property item declarations: *abstract properties*,
and *concrete properties*.  Abstract properties are defined on module level
and are not tied to any particular object type or link.  Concrete properties
are defined on specific object types.
