.. versionadded:: 3.0

.. _ref_sql_adapter:

===========
SQL adapter
===========

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
managed using ESDL (EdgeDB Schema Definition Language) and migration commands.

Any Postgres-compatible client can connect to an EdgeDB database by using the
same port that is used for the EdgeDB protocol and the
:versionreplace:`database;5.0:branch` name, username, and password already used
for the database.

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
    set the password <ref_sql_adapter_new_role>`, using those values to connect
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

.. _ref_sql_adapter_new_role:

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

When using inheritance, parent object types' tables will by default contain
all objects of both the parent type and any child types. The query below will
return all ``common::Content`` objects as well as all ``Movie`` objects.

.. code-block:: sql

    SELECT id, title FROM common."Content";

To omit objects of child types, use ``ONLY``. This query will return
``common::Content`` objects but not ``Movie`` objects.

.. code-block:: sql

    SELECT id, title FROM ONLY common."Content";

The SQL adapter supports a large majority of SQL language, including:

- ``SELECT`` and all read-only constructs (``WITH``, sub-query, ``JOIN``, ...),
- ``INSERT`` / ``UPDATE`` / ``DELETE``,
- ``COPY ... FROM``,
- ``SET`` / ``RESET`` / ``SHOW``,
- transaction commands,
- ``PREPARE`` / ``EXECUTE`` / ``DEALLOCATE``.

.. code-block:: sql

    SELECT id, 'Title is: ' || tittle
    FROM "Movie" m
    JOIN "Person" d ON m.director_id = d.id
    WHERE EXISTS (
        SELECT 1
        FROM "Movie.actors" act
        WHERE act.source = m.id
    );

The SQL adapter emulates the ``information_schema`` and ``pg_catalog`` views to
mimic the catalogs provided by Postgres 13.

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
   views. Because the EdgeDB SQL adapter does not allow writing or even
   creating schemas, view, or tables, any attempt to materialize dbt models
   will result in errors. If you want to build the models, we suggest first
   transferring your data to a true Postgres instance via pg_dump or Airbyte.
   Tests and previews can still be run directy against the EdgeDB instance.

.. _Postgres replication mechanism:
   https://www.postgresql.org/docs/current/runtime-config-replication.html
.. _XMIN Replication:
   https://www.postgresql.org/docs/15/ddl-system-columns.html


ESDL to PostgreSQL
==================

As mentioned, the SQL schema of the database is managed trough EdgeDB Schema
Definition Language (ESDL). Here is a breakdown of how each of the ESDL
construct is mapped to PostgreSQL schema:

- Objects types are mapped into tables.
  Each table has columns ``id UUID`` and ``__type__ UUID`` and one column for
  each single property or link.

- Single properties are mapped to tables columns.

- Single links are mapped to table columns with suffix ``_id`` and are of type
  ``UUID``. They contain the ids of the link's target type.

- Multi properties are mapped to tables with two columns:

  - ``source UUID``, which contains the id of the property's source object type,
  - ``target``, which contains values of the property.

- Multi links are mapped to tables with columns:

  - ``source UUID``, which contains the id of the property's source object type,
  - ``target UUID``, which contains the ids of the link's target object type,
  - one column for each link property, using the same rules as properties on
    object types.

- Aliases are not mapped to PostgreSQL schema.

.. versionadded:: 6.0

    - Globals are mapped to connection settings, prefixed with ``global``.
      For example, a ``global default::username: str`` can be accessed using:

      .. code-block:: sql

          SET "global default::username" TO 'Tom'``;
          SHOW "global default::username";

    - Access policies are applied to object type tables when setting
      ``apply_access_policies_pg`` is set to ``true``.

    - Mutation rewrites and triggers are applied to all DML commands.

DML commands
============

.. versionchanged:: _default

    Data Modification Language commands (``INSERT``, ``UPDATE``, ``DELETE``, ..)
    are not supported in EdgeDB <6.0.

.. versionchanged:: 6.0

.. versionadded:: 6.0

    When using ``INSERT``, ``DELETE`` or ``UPDATE`` on any table, mutation
    rewrites and triggers are applied. These commands do not have a
    straight-forward translation to EdgeQL DML commands, but instead use the
    following mapping:

    - ``INSERT INTO "Foo"`` object table maps to ``insert Foo``,

    - ``INSERT INTO "Foo.keywords"`` link/property table maps to an
      ``update Foo { keywords += ... }``,

    - ``DELETE FROM "Foo"`` object table maps to ``delete Foo``,

    - ``DELETE FROM "Foo.keywords"`` link property/table maps to
      ``update Foo { keywords -= ... }``,

    - ``UPDATE "Foo"`` object table maps to ``update Foo set { ... }``,

    - ``UPDATE "Foo.keywords"`` is not supported.


Connection settings
===================

SQL adapter supports most of PostgreSQL connection settings
(for example ``search_path``), in the same manner as plain PostgreSQL:

.. code-block:: sql

    SET search_path TO my_module;

    SHOW search_path;

    RESET search_path;

.. versionadded:: 6.0

    In addition, there are the following EdgeDB-specific settings:

    - settings prefixed with ``"global "`` set the values of globals.

      Because SQL syntax allows only string, integer and float constants in
      ``SET`` command, globals of other types such as ``datetime`` cannot be set
      this way.

      .. code-block:: sql

          SET "global my_module::hello" TO 'world';

      Special handling is in place to enable setting:
        - ``bool`` types via integers 0 or 1),
        - ``uuid`` types via hex-encoded strings.

      .. code-block:: sql

          SET "global my_module::current_user_id"
           TO "592c62c6-73dd-4b7b-87ba-46e6d34ec171";
          SET "global my_module::is_admin" TO 1;

      To set globals of other types via SQL, it is recommended to change the
      global to use one of the simple types instead, and use appropriate casts
      where the global is used.


    - ``allow_user_specified_id`` (default ``false``),

    - ``apply_access_policies_pg`` (default ``false``),

    Note that if ``allow_user_specified_id`` or ``apply_access_policies_pg`` are
    unset, they default to configuration set by ``configure current database``
    EdgeQL command.


Introspection
=============

The adapter emulates introspection schemas of PostgreSQL: ``information_schema``
and ``pg_catalog``.

Both schemas are not perfectly emulated, since they are quite large and
complicated stores of information, that also changed between versions of
PostgreSQL.

Because of that, some tools might show objects that are not queryable or might
report problems when introspecting. In such cases, please report the problem on
GitHub so we can track the incompatibility down.

Note that since the two information schemas are emulated, querying them may
perform worse compared to other tables in the database. As a result, tools like
``pg_dump`` and other introspection utilities might seem slower.


Locking
=======

.. versionchanged:: _default

    SQL adapter does not support ``LOCK`` in EdgeDB <6.0.

.. versionchanged:: 6.0

.. versionadded:: 6.0

    SQL adapter supports LOCK command with the following limitations:

    - it cannot be used on tables that represent object types with access
      properties or links of such objects,
    - it cannot be used on tables that represent object types that have child
      types extending them.

Query cache
===========

An SQL query is issued to EdgeDB, it is compiled to an internal SQL query, which
is then issued to the backing PostgreSQL instance. The compiled query is then
cached, so each following issue of the same query will not perform any
compilation, but just pass through the cached query.

.. versionadded:: 6.0

    Additionally, most queries are "normalized" before compilation. This process
    extracts constant values and replaces them by internal query parameters.
    This allows sharing of compilation cache between queries that differ in
    only constant values. This process is totally opaque and is fully handled by
    EdgeDB. For example:

    .. code-block:: sql

        SELECT $1, 42;

    ... is normalized to:

    .. code-block:: sql

        SELECT $1, $2;

    This way, when a similar query is issued to EdgeDB:

    .. code-block:: sql

        SELECT $1, 500;

    ... it normalizes to the same query as before, so it can reuse the query
    cache.

    Note that normalization process does not (yet) remove any whitespace, so
    queries ``SELECT 1;`` and ``SELECT 1 ;`` are compiled separately.


Known limitations
=================

Following SQL statements are not supported:

- ``CREATE``, ``ALTER``, ``DROP``,

- ``TRUNCATE``, ``COMMENT``, ``SECURITY LABEL``, ``IMPORT FOREIGN SCHEMA``,

- ``GRANT``, ``REVOKE``,

- ``OPEN``, ``FETCH``, ``MOVE``, ``CLOSE``, ``DECLARE``, ``RETURN``,

- ``CHECKPOINT``, ``DISCARD``, ``CALL``,

- ``REINDEX``, ``VACUUM``, ``CLUSTER``, ``REFRESH MATERIALIZED VIEW``,

- ``LISTEN``, ``UNLISTEN``, ``NOTIFY``,

- ``LOAD``.

Following functions are not supported:

- ``set_config``,
- ``pg_filenode_relation``,
- most of system administration functions.


Example: gradual transition from ORMs to EdgeDB
===============================================

When a project is using Object-Relational Mappings (e.g. SQLAlchemy, Django,
Hibernate ORM, TypeORM) and is considering the migration to EdgeDB, it might
want to execute the transition gradually, as opposed to a total rewrite of the
project.

In this case, the project can start the transition by migrating the ORM models
to EdgeDB Schema Definition Language.

For example, such Hibernate ORM model in Java:

.. code-block::

    @Entity
    class Movie {
        @Id
        @GeneratedValue(strategy = GenerationType.UUID)
        UUID id;

        private String title;

        @NotNull
        private Integer releaseYear;

        // ... getters and setters ...
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
SQL schema of EdgeDB match the SQL schema that the ORM expects.

When this match is accomplished, any query that used to work with the old, plain
PostgreSQL, should now also work with the EdgeDB. For example, we can execute
the following query:

.. code-block:: sql

    INSERT INTO "Movie" (title, releaseYear)
    VALUES ("Madagascar", 2012)
    RETURNING id, title, releaseYear;

To complete the migration, the data can be exported from our old database into
an ``.sql`` file, which can be import it into EdgeDB:

.. code-block:: bash

    $ pg_dump {your PostgreSQL connection params} \
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

For a detailed migration example, see repository
`edgedb/hibernate-example <https://github.com/edgedb/hibernate-example>`_.
