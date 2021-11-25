.. _ref_cheatsheet_cli:

Using the CLI
=============

To initialize a new project:

.. code-block:: bash

    $ edgedb project init

If an ``edgedb.toml`` file exists in the current directory, it will initialize
a new project according to the settings defined in it.

Otherwise, a new project will be initialized and an ``edgedb.toml`` file and
``dbschema`` directory will be generated. For details on using projects, see
the :ref:`dedicated guide <ref_guide_using_projects>`.

Once initialized, you can run the CLI commands below without additional
connection options. If you don't set up a project, you'll need to use
:ref:`flags <ref_cli_edgedb_connopts>` to specify the target instance for each
command.

----------


Explicitly create a new EdgeDB instance ``my_instance``:

.. code-block:: bash

    $ edgedb instance create my_instance


----------


Create a database:

.. code-block:: bash

    $ edgedb database create special_db
    OK: CREATE


----------


Configure passwordless access (such as to a local development database):

.. code-block:: bash

    $ edgedb configure insert Auth \
    > --comment 'passwordless access' \
    > --priority 1 \
    > --method Trust
    OK: CONFIGURE INSTANCE


----------


Configure access that checks password (with a higher priority):

.. code-block:: bash

    $ edgedb configure insert Auth \
    > --comment 'password is required' \
    > --priority 0 \
    > --method SCRAM
    OK: CONFIGURE INSTANCE


----------


Connect to the default project database:

.. code-block:: bash

    $ edgedb
    EdgeDB 1.0-beta.2+ga7130d5c7.cv202104290000 (repl 1.0.0-beta.2)
    Type \help for help, \quit to quit.
    edgedb>


----------


Connect to some specific database:

.. code-block:: bash

    $ edgedb -d special_db
    EdgeDB 1.0-beta.2+ga7130d5c7.cv202104290000 (repl 1.0.0-beta.2)
    Type \help for help, \quit to quit.
    special_db>
