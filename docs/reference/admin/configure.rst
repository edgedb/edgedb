.. _ref_eql_statements_configure:

Configure
=========

:eql-statement:


``configure`` -- change a server configuration parameter

.. versionchanged:: _default

    .. eql:synopsis::

        configure {session | current database | instance}
            set <parameter> := <value> ;
        configure instance insert <parameter-class> <insert-shape> ;
        configure {session | current database | instance} reset <parameter> ;
        configure {current database | instance}
            reset <parameter-class> [ filter <filter-expr> ] ;

.. versionchanged:: 5.0

    .. eql:synopsis::

        configure {session | current branch | instance}
            set <parameter> := <value> ;
        configure instance insert <parameter-class> <insert-shape> ;
        configure {session | current branch | instance} reset <parameter> ;
        configure {current branch | instance}
            reset <parameter-class> [ filter <filter-expr> ] ;


Description
-----------

This command allows altering the server configuration.

The effects of :eql:synopsis:`configure session` last until the end of the
current session. Some configuration parameters cannot be modified by
:eql:synopsis:`configure session` and can only be set by
:eql:synopsis:`configure instance`.

.. versionchanged:: _default

    :eql:synopsis:`configure current database` is used to configure an
    individual EdgeDB database within a server instance with the
    changes persisted across server restarts.

.. versionchanged:: 5.0

    :eql:synopsis:`configure current branch` is used to configure an
    individual EdgeDB branch within a server instance with the
    changes persisted across server restarts.

:eql:synopsis:`configure instance` is used to configure the entire EdgeDB
instance with the changes persisted across server restarts.  This variant
acts directly on the file system and cannot be rolled back, so it cannot
be used in a transaction block.

The :eql:synopsis:`configure instance insert` variant is used for composite
configuration parameters, such as ``Auth``.


Parameters
----------

:eql:synopsis:`<parameter>`
    The name of a primitive configuration parameter.  Available
    configuration parameters are described in the :ref:`ref_std_cfg`
    section.

:eql:synopsis:`<parameter-class>`
    The name of a composite configuration value class.  Available
    configuration classes are described in the :ref:`ref_std_cfg`
    section.

:eql:synopsis:`<filter-expr>`
    An expression that returns a value of type :eql:type:`std::bool`.
    Only configuration objects matching this condition will be affected.


Examples
--------

Set the ``listen_addresses`` parameter:

.. code-block:: edgeql

    configure instance set listen_addresses := {'127.0.0.1', '::1'};

Set the ``query_work_mem`` parameter for the duration of the session:

.. code-block:: edgeql

    configure instance set query_work_mem := <cfg::memory>'4MiB';

Add a Trust authentication method for "my_user":

.. code-block:: edgeql

    configure instance insert Auth {
        priority := 1,
        method := (insert Trust),
        user := 'my_user'
    };

Remove all Trust authentication methods:

.. code-block:: edgeql

    configure instance reset Auth filter Auth.method is Trust;
