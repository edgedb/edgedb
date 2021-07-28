.. _ref_eql_statements_configure:

CONFIGURE
=========

:eql-statement:


``CONFIGURE`` -- change a server configuration parameter

.. eql:synopsis::

    CONFIGURE {SESSION | CURRENT DATABASE | INSTANCE}
        SET <parameter> := <value> ;
    CONFIGURE INSTANCE INSERT <parameter-class> <insert-shape> ;
    CONFIGURE {SESSION | CURRENT DATABASE | INSTANCE} RESET <parameter> ;
    CONFIGURE {CURRENT DATABASE | INSTANCE}
        RESET <parameter-class> [ FILTER <filter-expr> ] ;


Description
-----------

This command allows altering the server configuration.

The effects of :eql:synopsis:`CONFIGURE SESSION` last until the end of the
current session. Some configuration parameters cannot be modified by
:eql:synopsis:`CONFIGURE SESSION` and can only be set by
:eql:synopsis:`CONFIGURE INSTANCE`.

:eql:synopsis:`CONFIGURE CURRENT DATABASE` is used to configure an
individual EdgeDB database within a server instance with the
changes persisted across server restarts.

:eql:synopsis:`CONFIGURE INSTANCE` is used to configure the entire EdgeDB
instance with the changes persisted across server restarts.  This variant
acts directly on the file system and cannot be rolled back, so it cannot
be used in a transaction block.

The :eql:synopsis:`CONFIGURE INSTANCE INSERT` variant is used for composite
configuration parameters, such as ``Auth``.


Parameters
----------

:eql:synopsis:`<parameter>`
    The name of a primitive configuration parameter.  Available
    configuration parameters are described in the :ref:`ref_admin_config`
    section.

:eql:synopsis:`<parameter-class>`
    The name of a composite configuration value class.  Available
    configuration classes are described in the :ref:`ref_admin_config`
    section.

:eql:synopsis:`<filter-expr>`
    An expression that returns a value of type :eql:type:`std::bool`.
    Only configuration objects matching this condition will be affected.


Examples
--------

Set the listen_addresses parameter:

.. code-block:: edgeql

    CONFIGURE INSTANCE SET listen_addresses := {'127.0.0.1', '::1'};

Set the query_work_mem parameter for the duration of the session:

.. code-block:: edgeql

    CONFIGURE SESSION SET query_work_mem := '4MB';

Set the same parameter, but for the current database:

.. code-block:: edgeql

    CONFIGURE CURRENT DATABASE SET query_work_mem := '4MB';

Add a Trust authentication method for "my_user":

.. code-block:: edgeql

    CONFIGURE INSTANCE INSERT Auth {
        priority := 1,
        method := (INSERT Trust),
        user := 'my_user'
    };

Remove all Trust authentication methods:

.. code-block:: edgeql

    CONFIGURE INSTANCE RESET Auth FILTER Auth.method IS Trust;
