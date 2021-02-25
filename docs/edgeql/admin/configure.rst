.. _ref_eql_statements_configure:

CONFIGURE
=========

:eql-statement:


``CONFIGURE`` -- change a server configuration parameter

.. eql:synopsis::

    CONFIGURE {SESSION | CURRENT DATABASE | SYSTEM}
        SET <parameter> := <value> ;
    CONFIGURE SYSTEM INSERT <parameter-class> <insert-shape> ;
    CONFIGURE {SESSION | CURRENT DATABASE | SYSTEM} RESET <parameter> ;
    CONFIGURE {CURRENT DATABASE | SYSTEM}
        RESET <parameter-class> [ FILTER <filter-expr> ] ;


Description
-----------

This command allows altering the server configuration.

The effects of :eql:synopsis:`CONFIGURE SESSION` last until the end of the
current session. Some configuration parameters cannot be modified by
:eql:synopsis:`CONFIGURE SESSION` and can only be set by
:eql:synopsis:`CONFIGURE SYSTEM`.

:eql:synopsis:`CONFIGURE CURRENT DATABASE` is used to configure an
individual EdgeDB database within a server instance with the
changes persisted across server restarts.

:eql:synopsis:`CONFIGURE SYSTEM` is used to configure the entire EdgeDB
instance with the changes persisted across server restarts.  This variant
acts directly on the file system and cannot be rolled back, so it cannot
be used in a transaction block.

The :eql:synopsis:`CONFIGURE SYSTEM INSERT` variant is used for composite
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

    CONFIGURE SYSTEM SET listen_addresses := {'127.0.0.1', '::1'};

Set the query_work_mem parameter for the duration of the session:

.. code-block:: edgeql

    CONFIGURE SESSION SET query_work_mem := '4MB';

Set the same parameter, but for the current database:

.. code-block:: edgeql

    CONFIGURE CURRENT DATABASE SET query_work_mem := '4MB';

Remove all Trust authentication methods:

.. code-block:: edgeql

    CONFIGURE SYSTEM RESET Auth FILTER Auth.method IS Trust;
