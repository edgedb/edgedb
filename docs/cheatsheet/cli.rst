.. _ref_cheatsheet_cli:

CLI
===

Create a database instance ``my_instance``:

.. code-block:: bash

    $ edgedb server init my_instance


----------


Create a database:

.. code-block:: bash

    $ edgedb -I my_instance create-database my_new_project
    OK: CREATE


----------


Create a new role (other than default ``edgedb``):

.. code-block:: bash

    $ edgedb -I my_instance create-superuser-role project
    OK: CREATE


----------


Configure passwordless access (such as to a local development database):

.. code-block:: bash

    $ edgedb -I my_instance configure insert Auth \
    > --comment 'passwordless access' \
    > --priority 1 \
    > --method Trust
    OK: CONFIGURE SYSTEM


----------


Set a password for a role:

.. code-block:: bash

    $ edgedb -I my_instance alter-role project --password
    New password for 'project':
    Confirm password for 'project':
    OK: ALTER


----------


Configure access that checks password (with a higher priority):

.. code-block:: bash

    $ edgedb -I my_instance configure insert Auth \
    > --comment 'password is required' \
    > --priority 0 \
    > --method SCRAM
    OK: CONFIGURE SYSTEM


----------


Configure a port for accessing ``my_new_project`` database using EdgeQL:

.. code-block:: bash

    $ edgedb -I my_instance configure insert Port \
    > --protocol edgeql+http \
    > --database my_new_project \
    > --address 127.0.0.1 \
    > --port 8889 \
    > --user project \
    > --concurrency 4
    OK: CONFIGURE SYSTEM


----------


.. _ref_cheatsheet_admin_graphql:

Configure a port for accessing ``my_new_project`` database using GraphQL:

.. code-block:: bash

    $ edgedb -I my_instance configure insert Port \
    > --protocol graphql+http \
    > --database my_new_project \
    > --address 127.0.0.1 \
    > --port 8888 \
    > --user project \
    > --concurrency 4
    OK: CONFIGURE SYSTEM


----------


Connect to the database:

.. code-block:: bash

    $ edgedb --user project --password -d my_new_project
    Password for 'project':
    EdgeDB 1.0-alpha.5+g83a2a4fac.d20200826
    Type "\?" for help.
    my_new_project>
