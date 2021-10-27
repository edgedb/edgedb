.. eql:section-intro-page:: edgeql

.. _ref_edgeql:

======
EdgeQL
======


.. toctree::
    :maxdepth: 3
    :hidden:

    strings
    literals
    sets
    paths
    type_operators
    functions
    operators
    json
    math
    parameters
    select
    insert
    update
    delete
    with
    for
    transactions
    output
    introspection/index

EdgeQL is a next-generation query language designed to match SQL in power and
surpass it in terms of clarity, brevity, and intuitiveness. It's used to query
the database, insert/update/delete data, modify/introspect the schema, manage
transactions, and more.

Design goals
------------

EdgeQL is a spiritual successor to SQL designed with a few core principles in
mind.

**Compatible with modern languages**. A jaw-dropping amount of effort has been
spend attempting to `bridge the gap <https://en.wikipedia.org/wiki/
Object%E2%80%93relational_impedance_mismatch>`_ between SQL's *relational*
paradigm and the *object-oriented* nature of modern programming languages.
EdgeDB sidesteps this problem by modeling data in an *object-relational* way.

**Strongly typed**. EdgeQL is *inextricably tied* to EdgeDB's rigorous
object-oriented type system. The type of all expressions is statically
inferred by EdgeDB.

**Easy to learn**. Postgres-flavored SQL contains `469 keywords
<https://www.postgresql.org/docs/current/sql-keywords-appendix.html>`_. By
comparison EdgeQL contains 80.

**Designed for programmers**. It uses ``{ curly braces }`` to define scopes and
nested structures and the *assignment operator* ``:=`` to set values. Plus it
contains a comprehensive standard library of functions, operators, and control
flow constructs.

.. **Compiles to SQL**. All EdgeQL queries, no matter how complex, compile to a
.. single PostgreSQL query under the hood. With the exception of ``GROUP BY``,
.. EdgeQL is equivalent to SQL in terms of power and expressivity.

**Easy deep querying**. EdgeDB's object-relational nature makes it painless
to write deep, performant queries that traverse links, no ``JOINs`` required.

**Composable**. `Unlike SQL
</blog/we-can-do-better-than-sql#lack-of-orthogonality>`_, EdgeQL's syntax is
readily composable; queries can be cleanly nested without worrying about
Cartesian explosion.


.. note::

  For a detailed writeup on the design of SQL, see `We Can Do Better Than SQL
  </blog/we-can-do-better-than-sql#lack-of-orthogonality>`_ on the EdgeDB
  blog.




.. _ref_eql_fundamentals_type_system:

Type System
-----------

EdgeQL is *inextricably tied* to EdgeDB's rigorous type system. For a full
breakdown of EdgeDB's typesystem, refer to :ref:`Data Model
docs <ref_datamodel_typesystem>`. For convenience, a brief overview of the type
system is presented below.

.. list-table::

    * - String type
      - :eql:type:`str`

    * - Boolean type
      - :eql:type:`bool`

    * - Numerical types
      - :eql:type:`int16` :eql:type:`int32` :eql:type:`int64`
        :eql:type:`float32` :eql:type:`float64` :eql:type:`bigint`
        :eql:type:`decimal`

    * - JSON type
      - :eql:type:`json`

    * - UUID type
      - :eql:type:`uuid`

    * - Binary data
      - :eql:type:`bytes`

    * - Temporal types
      - :eql:type:`datetime` :eql:type:`duration`
        :eql:type:`cal::local_datetime` :eql:type:`cal::local_date`
        :eql:type:`cal::local_time` :eql:type:`cal::relative_duration`

    * - Auto-incrementing ``int64``
      - :eql:type:`sequence`

    * - Array types
      - :eql:type:`array\<x\> <array>`

    * - Tuple types
      - :eql:type:`tuple\<x, y, ...\> <tuple>` or
        :eql:type:`tuple\<foo: x, bar: y, ...\> <tuple>`

    * - Object types
      - :ref:`Defined by the user <ref_datamodel_object_types>`, e.g. ``User``,
        ``BlogPost``


For a full breakdown of EdgeDB's typesystem, refer to :ref:`Data Model
<ref_datamodel_typesystem>`.


Follow along
------------

The best way to learn EdgeQL is to play with it! Use the `online EdgeQL shell
</tutorial>`_ to execute any and all EdgeQL snippets in the following pages. Or
follow the :ref:`Quickstart <ref_quickstart>` to spin up an EdgeDB instance on your computer, then open an :ref:`interactive shell <ref_cli_edgedb>`.
