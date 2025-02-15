.. _ref_cli_gel_configure:

=============
gel configure
=============

Configure the |Gel| server.

.. cli:synopsis::

    gel configure [<connection-options>] <action> \
        [<parameter> <value>] \
        [<parameter-class> --<property>=<value> ...]

Description
===========

:gelcmd:`configure` is a terminal command used to alter the
configuration of a |Gel| instance. There are three types of
configuration actions that can be performed.

Actions
=======

:cli:synopsis:`gel configure insert`
    Insert a new configuration entry for a setting that supports
    multiple configuration objects (e.g. Auth or Port).

:cli:synopsis:`gel configure set`
    Set a scalar configuration value.

:cli:synopsis:`gel configure reset`
    Reset an existing configuration entry or remove all values for an
    entry that supports multiple configuration objects.


Options
=======

Most of the options are the same across all of the different
configuration actions.

:cli:synopsis:`<connection-options>`
    See :ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<parameter>`
    The name of a primitive configuration parameter.  Available
    configuration parameters are described in the :ref:`ref_std_cfg`
    section.

:cli:synopsis:`<value>`
    A value literal for a given configuration parameter or configuration
    object property.

:cli:synopsis:`<parameter-class>`
    The name of a composite configuration value class.  Available
    configuration classes are described in the :ref:`ref_std_cfg`
    section.

:cli:synopsis:`--<property>=<value>`
    Set the :cli:synopsis:`<property>` of a configuration object to
    :cli:synopsis:`<value>`.
