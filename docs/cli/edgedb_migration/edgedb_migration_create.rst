.. _ref_cli_edgedb_migration_create:


=======================
edgedb migration create
=======================

The next step after setting up the desired target schema is creating a
migration script. This is done by invoking the following command:

.. cli:synopsis::

    edgedb migration create [<options>]

This will start an interactive tool that will provide the user with
suggestions based on the differences between the current branch (or database
for pre-v5) and the schema file. The prompts will look something like this:

.. code-block::

    did you create object type 'default::User'? [y,n,l,c,b,s,q,?]
    ?

    y - confirm the prompt, use the DDL statements
    n - reject the prompt
    l - list the DDL statements associated with prompt
    c - list already confirmed EdgeQL statements
    b - revert back to previous save point, perhaps previous question
    s - stop and save changes (splits migration into multiple)
    q - quit without saving changes
    h or ? - print help

Options
=======

The ``migration create`` command runs on the database it is connected
to. For specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.

:cli:synopsis:`--non-interactive`
    Do not prompt user for input. By default this works only if all
    the changes are "safe" unless :cli:synopsis:`--allow-unsafe` is
    also specified.

:cli:synopsis:`--allow-unsafe`
    Apply the most probable unsafe changes in case there are any.
    This is only useful in non-interactive mode.

:cli:synopsis:`--allow-empty`
    Create a new migration even if there are no changes. This is
    useful for creating migration stubs for data-only migrations.

:cli:synopsis:`--schema-dir=<schema-dir>`
    Directory where the schema files are located. Defaults to
    ``./dbschema``.

:cli:synopsis:`--squash`
    .. note::

        This CLI feature is compatible with EdgeDB server 3.0 and above.

    Squashes all your migrations into a single migration.
