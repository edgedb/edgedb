.. versionadded:: 3.0

.. _ref_sql_support:

=======================
SQL interface of EdgeDB
=======================

.. edb:youtube-embed:: 0KdY2MPb2oc

Connecting
==========

EdgeDB server supports PostgreSQL connection interface. It implements PostgreSQL
wire protocol as well as SQL query language.

As of EdgeDB 6.0, it also supports a subset of Data Modification Language,
namely INSERT, DELETE and UPDATE statements.

It does not, however, support PostgreSQL Data Definition Language
(e.g. ``CREATE TABLE``). This means that it is not possible to use SQL
connections to EdgeDB to modify its schema. Instead, the schema should be
managed using migrations and EdgeQL commands.

Any Postgres-compatible client can connect to your EdgeDB database by using the
same port that is used for the EdgeDB protocol and the
:versionreplace:`database;5.0:branch` name, username, and password you already
use for your database.

.. versionchanged:: _default

    Here's how you might connect to a local instance on port 10701 (determined
    by running ``edgedb instance list``) with a database ``edgedb`` using the
    ``psql`` CLI:

    .. code-block:: bash

        $ psql -h localhost -p 10701 -U edgedb -d edgedb

    You'll then be prompted for a password. If you don't have it, you can run
    ``edgedb instance credentials --insecure-dsn`` and grab it out of the DSN
    the command returns. (It's the string between the second colon and the "at"
    symbol: ``edgedb://edgedb:PASSWORD_IS_HERE@<host>:<port>/<database>``)

.. versionchanged:: 5.0

    Here's how you might connect to a local instance on port 10701 (determined
    by running ``edgedb instance list``) on a branch ``main`` using the
    ``psql`` CLI:

    .. code-block:: bash

        $ psql -h localhost -p 10701 -U edgedb -d main

    You'll then be prompted for a password. If you don't have it, you can run
    ``edgedb instance credentials --insecure-dsn`` and grab it out of the DSN
    the command returns. (It's the string between the second colon and the "at"
    symbol: ``edgedb://edgedb:PASSWORD_IS_HERE@<host>:<port>/<branch>``)

.. note::

    The insecure DSN returned by the CLI for EdgeDB Cloud instances will not
    contain the password. You will need to either :ref:`create a new role and
    set the password <ref_sql_support_new_role>`, using those values to connect
    to your SQL client, or change the password of the existing role, using that
    role name along with the newly created password.

    .. code-block:: edgeql-repl

        db> alter role edgedb {
        ...   set password := 'my-password'
        ... };
        OK: ALTER ROLE

.. warning::

    Connecting to an EdgeDB Cloud instance via a Postgres client requires SNI
    support which was introduced in libpq v14. If a Postgres client uses your
    system's libpq (``psql`` does), you can connect as long as your libpq
    version is 14+. To check your version, run ``psql --version`` or
    ``pg_config --version``.

    If you're on Windows and these do not work for you, you can instead
    navigate to ``bin`` under your Postgres installation location, right-click
    ``libpq.dll``, click "Properties," and find the version on the "Details"
    tab.

.. _ref_sql_support_new_role:

Creating a new role
-------------------

This works well to test SQL support, but if you are going to be using it on an
ongoing basis, you may want to create a new role and use it to authenticate
your SQL clients. Set a password when you create your role. Then, use the role
name as your user name when you connect via your SQL client.

.. code-block:: edgeql

    create superuser role sql {
      set password := 'your-password'
    };

.. versionchanged:: _default

    .. code-block:: bash

        $ psql -h localhost -p 10701 -U sql -d edgedb

.. versionchanged:: 5.0

    .. code-block:: bash

        $ psql -h localhost -p 10701 -U sql -d main

In this example, when prompted for the password, you would enter
``your-password``.

.. warning::

    EdgeDB server requires TLS by default, and this is also true for our SQL
    support. Make sure to require SSL encryption in your SQL tool or client
    when using EdgeDB's SQL support. Alternatively, you can disable the TLS
    requirement by setting the ``EDGEDB_SERVER_BINARY_ENDPOINT_SECURITY``
    environment variable to ``optional``.


Querying
========

Object types in your EdgeDB schema are exposed as regular SQL tables containing
all the data you store in your EdgeDB database.

If you have a database with the following schema:

.. code-block:: sdl

    module default {
        type Person {
            name: str;
        };

        type Movie extending common::Content {
            release_year: int32;
            director: Person;
            star: Person {
                role: str;
            };
            multi actors: Person {
                role: str;
            };
            multi labels: str;
        };
    }
    module common {
        type Content {
            title: str;
        };
    }

you can access your data after connecting using the following SQL queries:

.. code-block:: sql

    SELECT id, name FROM "Person";
    SELECT id, title, release_year, director_id, star_id FROM "Movie";

Because the link ``star`` has link properties, it has its own table.
``source`` is the ``id`` of the ``Movie``. ``target`` is the ``id`` of the
``Person``.

.. code-block:: sql

    SELECT source, target, role FROM "Movie.star";

Links are in separate tables.

.. code-block:: sql

    SELECT source, target, role FROM "Movie.actors";

Multi properties are in separate tables. ``source`` is the ``id`` of the Movie.
``target`` is the value of the property.

.. code-block:: sql

    SELECT source, target FROM "Movie.labels";

When types are extended, parent object types' tables will by default contain
all objects of both the type and any types extended by it. The query below will
return all ``common::Content`` objects as well as all ``Movie`` objects.

.. code-block:: sql

    SELECT id, title FROM common."Content";

To omit objects of extended types, use ``ONLY``. This query will return
``common::Content`` objects but not ``Movie`` objects.

.. code-block:: sql

    SELECT id, title FROM ONLY common."Content";

The SQL connector supports read-only statements and will throw errors if the
client attempts ``INSERT``, ``UPDATE``, ``DELETE``, or any DDL command. It
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
``pg_catalog`` views to mimic the catalogs provided by Postgres 13.

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
- `Airbyte <https://airbyte.com/>`_ [1]_
- `Fivetran <https://www.fivetran.com/>`_ [1]_
- `Hevo <https://hevodata.com/>`_ [1]_
- `Stitch <https://www.stitchdata.com/>`_ [1]_
- `dbt <https://www.getdbt.com/>`_ [2]_


.. [1] At the moment, EdgeDB does not support "Log replication" (i.e., using
   the `Postgres replication mechanism`_). Supported replication methods
   include `XMIN Replication`_, incremental updates using "a user-defined
   monotonically increasing id," and full table updates.
.. [2] dbt models are built and stored in the database as either tables or
   views. Because the EdgeDB SQL connector does not allow writing or even
   creating schemas, view, or tables, any attempt to materialize dbt models
   will result in errors. If you want to build the models, we suggest first
   transferring your data to a true Postgres instance via pg_dump or Airbyte.
   Tests and previews can still be run directy against the EdgeDB instance.

.. _Postgres replication mechanism:
   https://www.postgresql.org/docs/current/runtime-config-replication.html
.. _XMIN Replication:
   https://www.postgresql.org/docs/15/ddl-system-columns.html


Example: gradual transition from ORMs to EdgeDB
===============================================

When a project is using Object-Relational Mappings (e.g. SQLAlchemy, Django,
Hibernate ORM, TypeORM) and is considering the migration to EdgeDB, it might
want to execute the transition gradually, as opposed to a total rewrite of the
project.

In this case, the project can start the transition by migrating the ORM models
to EdgeDB Schema Definition Language.

For example, such Hibernate ORM model in Java: 

.. code-block:: java

    @Entity
    class Movie {
        @Id
        @GeneratedValue(strategy = GenerationType.UUID)
        UUID id;
        
        private String title;

        @NotNull
        private Integer releaseYear;

        ... getters and setters ...
    }

... would be translated to the following EdgeDB SDL:

.. code-block:: sdl

    type Movie {
        title: str;

        required releaseYear: int32;
    }

A new EdgeDB instance can now be created and migrated to the translated schema.
At this stage, EdgeDB will allow SQL connections to write into the ``"Movie"``
table, just as it would have been created with the following DDL command:

.. code-block:: sql

    CREATE TABLE "Movie" (
        id UUID PRIMARY KEY DEFAULT (...),
        __type__ UUID NOT NULL DEFAULT (...),
        title TEXT,
        releaseYear INTEGER NOT NULL
    );

When translating the old ORM model to EdgeDB SDL, one should aim to make the
SQL schema of EdgeDB as similar to the SQL schema that the ORM expects.

When this is complete, any query that used to work with the old, plain
PostgreSQL, should now also work with the EdgeDB. For example, we can execute
the following query:

.. code-block:: sql

    INSERT INTO "Movie" (title, releaseYear)
    VALUES ("Madagascar", 2012)
    RETURNING id, title, releaseYear

As the last step, we can export the data from our old database into an ``.sql``
file and then import it into EdgeDB:

.. code-block:: bash

    $ pg_dump {your PostgreSLQ connection params} \
        --data-only --inserts --no-owner --no-privileges \
        > dump.sql

    $ psql {your EdgeDB connection params} --file dump.sql

Now, the ORM can be pointed to EdgeDB instead of the old PostgreSQL database,
which has been fully replaced.

Arguably, the development of new features with the ORM is now more complex for
the duration of the transition, since the developer has to modify two model
definitions: the ORM and the EdgeDB schema.

But it allows any new models to use EdgeDB schema, EdgeQL and code generators
for the client language of choice. The ORM-based code can now also be gradually
rewritten to use EdgeQL, one model at the time.

.. TODO:
.. - old primary keys that are not uuid,
.. - old properties that are named id, __type__, source or target,
.. - renaming properties in the transition. 
