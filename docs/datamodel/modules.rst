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

- ``std``: all standard types, functions and other declarations;
- ``schema``: types describing the introspection schema;
- ``__graphql__``: GraphQL-related types


DDL
===

Module can be defined using the :eql:stmt:`CREATE MODULE` EdgeQL command.
