.. _ref_cli_edgedb_configure:

================
edgedb configure
================

Configure the EdgeDB server.

.. cli:synopsis::

    edgedb configure [<connection-options>] <action> \
        [<parameter> <value>] \
        [<parameter-class> --<property>=<value> ...]

Description
===========

``edgedb configure`` is a terminal command used to alter the
configuration of an EdgeDB instance. There are three types of
configuration actions that can be performed.

Actions
=======

:cli:synopsis:`edgedb configure insert`
    Insert a new configuration entry for a setting that supports
    multiple configuration objects (e.g. Auth or Port).

:cli:synopsis:`edgedb configure set`
    Set a scalar configuration value.

:cli:synopsis:`edgedb configure reset`
    Reset an existing configuration entry or remove all values for an
    entry that supports multiple configuration objects.


Options
=======

Most of the options are the same across all of the different
configuration actions.

:cli:synopsis:`<connection-options>`
    See :ref:`connection options <ref_cli_edgedb_connopts>`.

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
