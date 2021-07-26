.. _ref_cheatsheet_cli:

CLI
===

To create a new database instance ``my_project`` for a project run
this in the project directory, then follow the prompts:

.. code-block:: bash

    $ edgedb project init my_instance

It will set up a new instance and associate that instance with the
project directory so that ``edgedb`` commands that run from this
directory will automatically connect to the project's instance.


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
