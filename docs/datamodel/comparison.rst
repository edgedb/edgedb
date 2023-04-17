.. _ref_datamodel_comparison:

===============
vs SQL and ORMs
===============

EdgeDB's approach to schema modeling builds upon the foundation of SQL while
taking cues from modern tools like ORM libraries. Let's see how it stacks up.

.. _ref_datamodel_sql_comparison:

Comparison to SQL
-----------------

When using SQL databases, there's no convenient representation of the schema.
Instead, the schema only exists as a series of ``{CREATE|ALTER|DELETE} {TABLE|
COLUMN}`` commands, usually spread across several SQL migration scripts.
There's no simple way to see the current state of your schema at a glance.

Moreover, SQL stores data in a *relational* way. Connections between tables are
represented with foreign key constraints and ``JOIN`` operations are required
to query across tables.

.. code-block::

  CREATE TABLE people (
    id            uuid  PRIMARY KEY,
    name          text,
  );
  CREATE TABLE movies (
    id            uuid  PRIMARY KEY,
    title         text,
    director_id   uuid  REFERENCES people(id)
  );

In EdgeDB, connections between tables are represented with :ref:`Links
<ref_datamodel_links>`.

.. code-block:: sdl
    :version-lt: 3.0

    type Movie {
      required property title -> str;
      required link director -> Person;
    }

    type Person {
      required property name -> str;
    }

.. code-block:: sdl

    type Movie {
      required title: str;
      required director: Person;
    }

    type Person {
      required name: str;
    }

This approach makes it simple to write queries that traverse this link, no
JOINs required.

.. code-block:: edgeql

  select Movie {
    title,
    director: {
      name
    }
  }

.. _ref_datamodel_orm_comparison:

Comparison to ORMs
------------------

Object-relational mapping libraries are popular for a reason. They provide a
way to model your schema and write queries in a way that feels natural in the
context of modern, object-oriented programming languages. But ORMs have
downsides too.

- **Lock-in**. Your schema is strongly coupled to the ORM library you are
  using. More generally, this also locks you into using a particular
  programming language.
- Most ORMs have more **limited querying capabilities** than the query
  languages they abstract.
- Many ORMs produce **suboptimal queries** that can have serious performance
  implications.
- **Migrations** can be difficult. Since most ORMs aim to be the single source
  of truth for your schema, they necessarily must provide some sort of
  migration tool. These migration tools are maintained by the contributors to
  the ORM library, not the maintainers of the database itself. Quality control
  and long-term maintenance is not always guaranteed.

From the beginning, EdgeDB was designed to incorporate the best aspects of ORMs
— declarative modeling, object-oriented APIs, and intuitive querying —
without the drawbacks.
