.. versionadded:: 3.0

.. _ref_sql_support:

=====================
SQL support in EdgeDB  
=====================

EdgeDB supports running SQL queries via the Postgres protocol. Any
Postgres-compatible client can connect to your EdgeDB database by using the
same port that is used for the EdgeDB protocol and the same database name,
username, and password you already use for your database.

.. warning::

    Make sure to require SSL encryption when using SQL support

Object types in your EdgeDB schema are exposed as regular SQL tables containing
all the data you store in your EdgeDB database.

If you have a database with the following schema: 

.. code-block:: sdl

    module default {
        type Person {
            name: str,
        };

        type Movie extending common::Content {
            release_year: int32,
            director: Person,
            star: Person {
                role: str,
            },
            multi actors: Person {
                role: str,
            },
            multi labels: str,
        };
    }
    module common {
        type Content {
            title: str,
        };
    }

you can access your data after connecting using the following SQL queries:

.. code-block:: sql

    SELECT id, name FROM "Person";
    SELECT id, title, release_year, director_id, star_id FROM "Movie";

    -- Because the link `star` has link properties, it has its own table.
    -- `source` is the id of the Movie.
    -- `target` is the id of the Person.
    SELECT source, target, role FROM "Movie.star";

    -- Links are in separate tables.
    SELECT source, target, role FROM "Movie.actors";

    -- Multi properties are in separate tables.
    -- `source` is the id of the Movie.
    -- `target` is the value of the property.
    SELECT source, target FROM "Movie.labels";

    -- When types are extended, parent object types' tables will by default
    -- contain all objects of both the type and any types extended by it.
    -- The query below will return all `common::Content` objects as well as all
    -- `Movie` objects.
    SELECT id, title FROM common."Content";

    -- To omit objects of extended types, use `ONLY`.
    -- This query will return `common::Content` objects but not `Movie`
    -- objects.
    SELECT id, title FROM ONLY common."Content";

The SQL connector supports read-only statements and will throw errors if the
client attemps ``INSERT``, ``UPDATE``, ``DELETE``, or any DDL command. It
supports all SQL expressions supported by Postgres.

.. code-block:: sql

    SELECT id, 'Title is: ' || tittle
    FROM "Movie" m
    JOIN "Person" d ON m.director_id = d.id
    WHERE EXISTS (
        SELECT 1
        FROM "Movie.actors" act
        WHERE act.source = m.id
    );

EdgeDB accomplishes this by emulating the ``information_schema`` and
``pg_catalog`` views to mimick the catalogs provided by Postgres 13.

.. note::

    Learn more about the Postgres information schema from `the Postgres
    information schema documentation
    <https://www.postgresql.org/docs/13/information-schema.html>`_.

.. warning::

    Some tables may be truncated and may not contain all objects you would
    expect a true Postgres instance to contain. This may be a source of
    problems when using tools that introspect the database and rely on internal
    Postgres features.

Tested SQL tools
================

- `pg_dump <https://www.postgresql.org/docs/13/app-pgdump.html>`_
- `Metabase <https://www.metabase.com/>`_
- `Cluvio <https://www.cluvio.com/>`_
- `Tableau <https://www.tableau.com/>`_
- `DataGrip <https://www.jetbrains.com/datagrip/>`_
- `Airbyte <https://airbyte.com/>`_
- `Fivetran <https://www.fivetran.com/>`_
- `Hevo <https://hevodata.com/>`_
- `Stitch <https://www.stitchdata.com/>`_
- `dbt <https://www.getdbt.com/>`_ [#]_


.. [#] dbt models are built and stored in the database as either tables or
   views. Because the EdgeDB SQL connector does not allow writing or even
   creating schemas, view, or tables, any attempt to materialize dbt models
   will result in errors. If you want to build the models, we suggest first
   transferring your data to a true Postgres instance via pg_dump or Airbyte.
   Tests and previews can still be run directy against the EdgeDB instance.
