.. _ref_cli_gel_migration_edit:


==================
gel migration edit
==================

Edit migration file.

.. cli:synopsis::

    gel migration edit [<options>]

Invokes ``$EDITOR`` on the last migration file, and then fixes migration id
after editor exits. Usually should be used for migrations that haven't been
applied yet.

Options
=======

The ``migration edit`` command runs on the database it is connected to. For
specifying the connection target see :ref:`connection options
<ref_cli_gel_connopts>`.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--no-check`
    Do not check migration within the database connection.

:cli:synopsis:`--non-interactive`
    Fix migration id non-interactively, and don't run editor.
