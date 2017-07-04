.. _ref_tutorial_core:

Core features of EdgeDB
=======================

Everything is a set
-------------------

EdgeDB is fundamentally working with sets. Which means that there can
be no duplicate results. The *identity* of an object is determined by
its ``id``. For practical reasons there's a caveat for atomic values,
whose identity is defined as being *always* unique (essentially every
instance of the atomic value is a different unique entity as far as
sets are concerned). Please see the chapter on
:ref:`set operators<ref_edgeql_expressions_setops>` for more
examples and details.

All sets must also be homogeneous, i.e. all members of a set have to
be of the same basic :ref:`type<ref_edgeql_types>`. Thus all sets are
either composed of *objects*, *atomic values* or
:ref:`tuples<ref_edgeql_types_tuples>`. It's worth noting that mixing
objects representing different
:ref:`concepts<ref_schema_architechture_concepts>` is fine
since they are all derived from the same base ``Object``.

For more details see :ref:`how expressions work<ref_edgeql_expressions>`.


There is no NULL
----------------

Traditional relational DBs deal with tables and use ``NULL`` as a
value denoting absence of data. Thus ``NULL`` is a special *value* in
those DBs. EdgeDB works with *sets*, so when a link/relationship is
missing, there is no actual value associated with it, instead it's
just an empty set.
