.. _ref_datamodel_modules:

=======
Modules
=======

All elements of a database schema are grouped into logical units
called *modules*.  A module has a name that is unique inside a database.
The same schema object name can be used in different modules without
conflict.  For example, both ``module1`` and ``module2`` can contain
a ``User`` object type.

Schema objects can be referred to by a fully-qualified name using the
``<module>::<name>`` notation.

Every EdgeDB schema contains the following standard modules:

- ``std``: standard types, functions and other declarations
- ``schema``: types describing the :ref:`introspection <ref_eql_introspection>`
  schema
- ``sys``: system-wide entities, such as user roles and
  :ref:`databases <ref_datamodel_databases>`
- ``cfg``: configuration and settings
- ``stdgraphql``: GraphQL-related types
- ``math``: algebraic and statistical functions
- ``default``: the default module for user-defined types, functions, etc.


See Also
--------

:ref:`SDL <ref_eql_sdl_modules>`,
:eql:stmt:`CREATE MODULE`,
:eql:stmt:`DROP MODULE`.
