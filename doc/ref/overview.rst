What is EdgeDB?
===============

Overview
--------

We believe that the data model is the most important part of any
application. Real-world applications can easily have hundreds of
distinct concepts and thousands of relations between them. When
correctness policies, access policies, behaviours and various
associated metadata is added to that, the data model can suddenly
become hard to maintain with every change only worsening the
situation.

Our approach to handle this challenge is to make data schema
declaration a concise, simple and precise document which is easy to
read and maintain. Every aspect inherent to the data architecture, be
it data domain constraints, value defaults, access policy, must go to
this schema. This schema is then held as the ultimate authoritative
source of data architecture description and its requirements are
automatically enforced throughout the application, from the database
to front-end input validation.

This approach guarantees structure consistency and greatly reduces the
overhead of making changes to the schema.


Fundamental model
-----------------

The fundamental data model of EdgeDB is a semantic network, which is a
directed graph with nodes representing data and edges representing
semantic relationships between the data.

For example, a *City* can relate to a *Country* through the "capital"
relationship, and *Country* relates to *City* through the "country"
relationship.

.. aafig::
    :aspect: 60
    :scale: 150
    :textual:

            +-------+{capital}+------+
            |                        |
     +------+------+           +-----v-----+
     |             |           |           |
     |   Country   |           |    City   |
     |             |           |           |
     +------^------+           +-----+-----+
            |                        |
            +-------+{country}+------+

Fundamentally, there are two kinds of nodes: *concepts* and *atoms*.
Concept is a node that can have both incoming and outgoing links.
Atoms, on the other hand, cannot ever have outgoing links and thus are
terminal. Atoms are meant to represent simple scalar values, whereas
concepts are meant to represent composite objects.


.. _ref_overview_set:

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
