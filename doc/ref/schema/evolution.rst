.. _ref_schema_evolution:

Schema evolution
----------------

EdgeDB provides powerful mechanisms for schema evolution and
versioning. Since the data schema is fully represented in an abstract
form in EdgeDB, it is possible to perform complex analysis and
modification of the schema in automatic mode.

When it comes to schema modifications, the most common workflow of an
application developer is to modify the source schema document and then
use EdgeDB commands to examine and commit schema changes.
Schema changes are stored internally as deltas just like in common SCM
systems. The deltas, in fact, are scripts that are interpreted by the
EdgeDB schema machine to mutate the schema.

Deltas are always module-specific and can specify dependencies on
other modules. Essentially the dependencies specify which deltas must
be applied in other modules for a given delta to be valid.


Schema modules
~~~~~~~~~~~~~~

For convenience EdgeDB framework considers all the schema files in the
same directory describing one module. Since the schema is a
declarative construct, there is no need to consider the order in which
various elements are declared and so the exact way in which these
declarations are split among one or more files is not important and is
only reflective of convenience. Schema evolution operations deal with
the entire module as a single unit.

The simplest example of schema evolution is a situation when the
entire schema and the changes are all contained within one single
module (adding or removing ``concepts``, ``atoms``, ``links``, etc.).
In this case the state of this module represents the state of the
underlying DB schema.

The original state of the module can be represented by a delta from an
empty DB to the starting state. This delta must have a name that is
unique for the module. This name is used in specifying module
dependencies. It is also used to determine the overall state of the
EdgeDB schema. For example, consider the following schema:

.. code-block:: eschema

    concept City extends NamedObject

    concept Country extends NamedObject

Let's say that this is the starting state of the module ``geo`` called
``geo::v1``. It is very basic and defines a ``City`` and ``Country``
objects as some entities with a name, but doesn't define any
relationship between them. This shortcoming is addressed in the next
iteration of development and the new module schema looks like this:

.. code-block:: eschema

    # module geo

    concept City extends NamedObject:
        link country to Country:
            mapping: *1

    concept Country extends NamedObject:
        link capital to City:
            mapping: 11

Adding the changes and running the migration command on the new schema
we will get a new delta. This delta needs a name, say ``geo::v2``, and
it has an implicit dependency on the previous delta of this module:
``geo:v1``.

Suppose we want to introduce further details and now want to add a
``mayor`` link for each ``City`` and this link will point to a concept
``Person`` defined in a different module. First we need to add this
new module to our overall schema:

.. code-block:: eschema

    # module subjects

    concept Person:
        link first_name to str
        link last_name to str

Adding the new module will generate a delta with no dependencies (so
far), which we will call ``subjects::v1``. At this point the overall
state of the EdgeDB schema can be described by the set of modules that
it is made of: ``geo::v2`` and ``subjects::v1``. Now that we have the
2 modules defined, let's add the ``mayor`` link to the ``City``.


.. code-block:: eschema

    # module geo

    import subjects

    concept City extends NamedObject:
        link country to Country:
            mapping: *1
        link mayor to subjects::Person:
            mapping: *1

    concept Country extends NamedObject:
        link capital to City:
            mapping: 11

We import the module ``subjects`` into geo and declare the ``mayor``
link pointing to ``subjects::Person``. The corresponding delta
``geo::v3`` would now depend on ``geo::v2`` and ``subjects::v1``. In
order for the overall schema to be valid all modules must satisfy all
of their dependencies. If we further evolve the module ``subject`` to
a new state ``subject::v2``, we will need to add a delta for the
module ``geo`` that will update the dependencies from ``geo::v2``,
``subject::v1`` to ``geo::v3``, ``subject::v2`` in order to keep the
schema valid. This means that in order to migrate the schema EdgeDB
will require both deltas ``subject::v2`` and ``geo::v4`` and it will
determine the order in which they need to be applied based on the
declared dependencies.

.. aafig::
    :aspect: 60
    :scale: 150

        +-------+     +-------+     +-------+          +-------+
        |geo::v1+---->+geo::v2+---->+geo::v3+--------->+geo::v4|
        +-------+     +-------+     ++------+          ++------+
                                     ^                  ^
                                     |                  |
                                     +                  +
                                    /                  /
                      +------------+     +------------+
                      |subjects::v1+---->+subjects::v2|
                      +------------+     +------------+


EdgeDB can determine that in order to correctly initialize an empty DB
to the final state of ``{geo::v4, subjects::v2}`` the deltas need to
be applied in the following order given by the linearization of the
dependency graph:

::

    geo::v1, geo::v2, subjects::v1, geo::v3, subjects::v2, geo::v4
