.. _ref_eql_statements_configure:

Configure
=========

:eql-statement:


``configure`` -- change a server configuration parameter

.. eql:synopsis::

    configure {session | current branch | instance}
        set <parameter> := <value> ;
    configure instance insert <parameter-class> <insert-shape> ;
    configure {session | current branch | instance} reset <parameter> ;
    configure {current branch | instance}
        reset <parameter-class> [ filter <filter-expr> ] ;

.. note::
    Prior to |Gel| and |EdgeDB| 5.0 *branches* were called *databases*.
    ``configure current branch`` is used to be called
    ``configure current database``, which is still supported for backwards
    compatibility.


Description
-----------

This command allows altering the server configuration.

The effects of :eql:synopsis:`configure session` last until the end of the
current session. Some configuration parameters cannot be modified by
:eql:synopsis:`configure session` and can only be set by
:eql:synopsis:`configure instance`.

:eql:synopsis:`configure current branch` is used to configure an
individual Gel branch within a server instance with the
changes persisted across server restarts.

:eql:synopsis:`configure instance` is used to configure the entire Gel
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
