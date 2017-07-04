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
