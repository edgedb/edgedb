What is EdgeDB?
===============

Overview
--------

We believe that the data model is the most important part of any non-toy
application. Real-world applications can easily have hundreds of
distinct concepts and thousands of relations between them. When
correctness policies, access policies, behaviours and various
associated metadata is added to that, the data model can easily become
an unmaintainable mess with every change only worsening the situation.

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

The fundamental data model of EdgeDB is a semantic network, which is
represented by a directed graph ``G := (V, E)``, where ``V`` is a set
of vertices which represent *nodes* and ``E`` is a set of edges which
represent *semantic relationships* between nodes.

For example, a City can relate to a Country through the "capital"
relationship, and Country relates to City through the "country"
relationship.

::

  [Country] -- capital --> [City]
     ^                        |
     \-------- country -------/

Fundamentally, there are two kinds of nodes: *concepts* and *atoms*.
Concept is a composite node that can have both incoming and outgoing
links. Atoms, on the other hand, are terminal, i.e. atoms can only
have incoming links. Most of the time atoms hold simple scalar values,
like strings or numbers, but that is not a strict requirement. For
example, a Country can have a string atom linked through the "name"
relationship:

::

  [Country] -- name --> string
