.. _ref_cli_gel_restore:


===========
gel restore
===========

Restore a |Gel| |branch| from a backup file.

.. cli:synopsis::

    gel restore [<options>] <path>


Description
===========

:gelcmd:`restore` is a terminal command used to restore an Gel |branch|
|branch| from a backup file. The backup is restored to the
currently active branch.

.. note::

    The backup cannot be restored to a |branch| with any
    existing schema. As a result, you should restore to one of these targets:

    - a new empty |branch| which can be created using
      :ref:`ref_cli_gel_branch_create` with the ``--empty`` option
    - a new empty |branch| if your instance is running |EdgeDB| versions
      prior to 5
    - an existing |branch| that has been wiped with the appropriate
      ``wipe`` command (either :ref:`ref_cli_gel_branch_wipe` or
      :ref:`ref_cli_gel_database_wipe`; note that this will destroy all data
      and schema currently in that branch/database)


Options
=======

The ``restore`` command restores the backup file into the active |branch|.
For specifying the connection target see :ref:`connection options
<ref_cli_gel_connopts>`.

:cli:synopsis:`<path>`
    The name of the backup file to restore the |branch| from.

:cli:synopsis:`--all`
    Restore all |branches| and the server configuration
    using the directory specified by the :cli:synopsis:`<path>`.

:cli:synopsis:`-v, --verbose`
    Verbose output.
