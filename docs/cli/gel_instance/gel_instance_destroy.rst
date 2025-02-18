.. _ref_cli_gel_instance_destroy:


====================
gel instance destroy
====================

Remove a |Gel| instance.

.. cli:synopsis::

     gel instance destroy [<options>] <name>


Description
===========

:gelcmd:`instance destroy` is a terminal command for removing an (or |gel.toml|)
instance and all its data.

.. note::

    The :gelcmd:`instance destroy` command is not intended for use with
    self-hosted instances.


Options
=======

:cli:synopsis:`<name>`
    The |Gel| instance name.

:cli:synopsis:`--force`
    Destroy the instance even if it is referred to by a project.

:cli:synopsis:`-v, --verbose`
    Verbose output.
