.. _ref_migration_recovering:

==========================
Recovering lost migrations
==========================

Each time you create a migration with :ref:`ref_cli_edgedb_migration_create`,
a file containing the DDL for that migration is created in
``dbschema/migrations``. When you apply a migration with
:ref:`ref_cli_edgedb_migration_apply` or :ref:`ref_cli_edgedb_migrate`, the
database stores a record of the migration it applied.

On rare occasions, you may find you have deleted your migration files by
mistake. If you don't care about any of your data and don't need to keep your
migration history, you can :ref:`wipe <ref_cli_edgedb_database_wipe>` your
database and start over, creating a single migration to the current state of
your schema. If that's not an option, all hope is not lost. You can instead
recover your migrations from the database.

Run this query to see your migrations:

.. code-block:: edgeql

    select schema::Migration {
      name,
      script,
      parents: {name}
    }

You can rebuild your migrations from the results of this query, either manually
or via a script if you've applied too many of them to recreate by hand.
Migrations in the file system are named sequentially starting from
``00001.edgeql``. They are in this format:

.. code-block:: edgeql

    CREATE MIGRATION m1rsm66e5pvh5ets2yznutintmqnxluzvgbocspi6umd3ht64e4naq
                     # ☝️ Replace with migration name
        ONTO m1l5esbbycsyqcnx6udxx24riavvyvkskchtekwe7jqx5mmiyli54a
             # ☝️ Replace with parent migration name
    {
      # script
      # ☝️ Replace with migration script
    };

or if this is the first migration:

.. code-block:: edgeql

    CREATE MIGRATION m1l5esbbycsyqcnx6udxx24riavvyvkskchtekwe7jqx5mmiyli54a
                     # ☝️ Replace with migration name
        ONTO initial
    {
      # script
      # ☝️ Replace with migration script
    };

Replace the name, script, and parent name with the values from
your ``Migration`` query results.

You can identify the first migration in your query results as the one with no
object linked on ``parents``. Order the other migrations by chaining the links.
The ``Migration`` with the initial migration linked via ``parents`` is the
second migration — ``00002.edgeql``. The migration linking to the second
migration via ``parents`` is the third migration, and so on).
