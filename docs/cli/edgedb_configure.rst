.. _ref_cli_edgedb_configure:

================
edgedb configure
================

Configure the EdgeDB server.

.. cli:synopsis::

    edgedb [<connection-option>...] configure [ <option> ... ] \
        set <parameter> <value>

    edgedb [<connection-option>...] configure [ <option> ... ] \
        reset <parameter>

    edgedb [<connection-option>...] configure [ <option> ... ] \
        insert <parameter-class> [ --<property>=<value> ... ]

    edgedb [<connection-option>...] configure [ <option> ... ] \
        reset <parameter-class> [ --<property>=<value> ... ]


Description
===========

``edgedb configure`` is a terminal command used to alter the configuration
of an EdgeDB instance.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.

:cli:synopsis:`<parameter>`
    The name of a primitive configuration parameter.  Available
    configuration parameters are described in the :ref:`ref_admin_config`
    section.

:cli:synopsis:`<value>`
    A value literal for a given configuration parameter or configuration
    object property.

:cli:synopsis:`<parameter-class>`
    The name of a composite configuration value class.  Available
    configuration classes are described in the :ref:`ref_admin_config`
    section.

:cli:synopsis:`--<property>=<value>`
    Set the :cli:synopsis:`<property>` of a configuration object to
    :cli:synopsis:`<value>`.
