.. _ref_cheatsheet_admin:

Admin Commands
==============

Create a database:

.. code-block:: edgeql-repl

    db> CREATE DATABASE my_new_project;
    CREATE

Create a role:

.. code-block:: edgeql-repl

    db> CREATE SUPERUSER ROLE project;
    CREATE

Configure passwordless access (such as to a local development database):

.. code-block:: edgeql-repl

    db> CONFIGURE SYSTEM INSERT Auth {
    ...     # Human-oriented comment helps figuring out
    ...     # what authentication methods have been setup
    ...     # and makes it easier to identify them.
    ...     comment := 'passwordless access',
    ...     priority := 1,
    ...     method := (INSERT Trust),
    ... };
    CONFIGURE SYSTEM

Set a password for a role:

.. code-block:: edgeql-repl

    db> ALTER ROLE project
    ...     SET password := 'super-password';
    ALTER

Configure access that checks password (with a higher priority):

.. code-block:: edgeql-repl

    db> CONFIGURE SYSTEM INSERT Auth {
    ...     comment := 'password is required',
    ...     priority := 0,
    ...     method := (INSERT SCRAM),
    ... };
    CONFIGURE SYSTEM

Remove a specific authentication method:

.. code-block:: edgeql-repl

    db> CONFIGURE SYSTEM RESET Auth
    ... FILTER .comment = 'password is required';
    CONFIGURE SYSTEM

Configure a port for accessing ``my_new_project`` database using EdgeQL:

.. code-block:: edgeql-repl

    db> CONFIGURE SYSTEM INSERT Port {
    ...     protocol := "edgeql+http",
    ...     database := "my_new_project",
    ...     address := "127.0.0.1",
    ...     port := 8888,
    ...     user := "http",
    ...     concurrency := 4,
    ... };
    CONFIGURE SYSTEM

.. _ref_cheatsheet_admin_graphql:

Configure a port for accessing ``my_new_project`` database using GraphQL:

.. code-block:: edgeql-repl

    db> CONFIGURE SYSTEM INSERT Port {
    ...     protocol := "graphql+http",
    ...     database := "my_new_project",
    ...     address := "127.0.0.1",
    ...     port := 8888,
    ...     user := "http",
    ...     concurrency := 4,
    ... };
    CONFIGURE SYSTEM

Run a script from command line:

.. cli:synopsis::

    cat myscript.edgeql | edgedb [<connection-option>...]
