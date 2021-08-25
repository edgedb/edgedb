.. _ref_cheatsheet_admin:

Administration
==============

Create a database:

.. code-block:: edgeql-repl

    db> CREATE DATABASE my_new_project;
    CREATE


----------


Create a role:

.. code-block:: edgeql-repl

    db> CREATE SUPERUSER ROLE project;
    CREATE


----------


Configure passwordless access (such as to a local development database):

.. code-block:: edgeql-repl

    db> CONFIGURE INSTANCE INSERT Auth {
    ...     # Human-oriented comment helps figuring out
    ...     # what authentication methods have been setup
    ...     # and makes it easier to identify them.
    ...     comment := 'passwordless access',
    ...     priority := 1,
    ...     method := (INSERT Trust),
    ... };
    CONFIGURE INSTANCE


----------


Set a password for a role:

.. code-block:: edgeql-repl

    db> ALTER ROLE project
    ...     SET password := 'super-password';
    ALTER


----------


Configure access that checks password (with a higher priority):

.. code-block:: edgeql-repl

    db> CONFIGURE INSTANCE INSERT Auth {
    ...     comment := 'password is required',
    ...     priority := 0,
    ...     method := (INSERT SCRAM),
    ... };
    CONFIGURE INSTANCE


----------


Remove a specific authentication method:

.. code-block:: edgeql-repl

    db> CONFIGURE INSTANCE RESET Auth
    ... FILTER .comment = 'password is required';
    CONFIGURE INSTANCE


----------


Run a script from command line:

.. cli:synopsis::

    cat myscript.edgeql | edgedb [<connection-option>...]
