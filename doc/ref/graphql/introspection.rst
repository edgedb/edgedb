.. _ref_graphql_introspection:


Introspection
=============

GraphQL introspection can be used to explore EdgeDB schema, although
it may not be 100% exhaustive. Since EdgeDB supports GraphQL queries
expressing filtering conditions with arbitrary nesting depth it is not
practical or even possible to provide an exhaustive list of all the
field arguments for all of the *queries* and *mutations*.
Introspection will only provide the basic fields that are immediately
associated with a specific *Concept* in EdgeDB. It is a matter of
convention how these field names can be combined in order to produce
field arguments.
