.. _ref_datamodel_constraints:

===========
Constraints
===========

*Constraints* are an EdgeDB mechanism that provides fine-grained control
over which data is considered valid.  A constraint may be defined on a
:ref:`scalar type <ref_datamodel_scalar_types>`, an
:ref:`object type <ref_datamodel_object_types>`, a
:ref:`concrete link <ref_datamodel_links>`, or a
:ref:`concrete property <ref_datamodel_props>`.  In case of a
constraint on a scalar type, the *subjects* of the constraint are
the instances of that scalar, thus the values that the scalar can
take will be restricted.  Whereas for link or property constraints
the *subjects* are the targets of those links or properties,
restricting what objects or values those links and properties may
reference.  The *subject* of a constraint can be referred to in
the constraint expression as ``__subject__``.


See Also
--------

Constraint
:ref:`SDL <ref_eql_sdl_constraints>`,
:ref:`DDL <ref_eql_ddl_constraints>`,
:ref:`introspection <ref_eql_introspection_constraints>`, and
constraints defined in the :ref:`standard library <ref_std_constraints>`.
