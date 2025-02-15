.. _ref_cli_gel_branch_rename:


=================
gel branch rename
=================

Rename a :ref:`branch <ref_datamodel_branches>`

.. cli:synopsis::

    gel branch rename [<options>] <old-name> <new-name>


Options
=======

The ``branch rename`` command runs in the |Gel| instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<old-name>`
    The current name of the branch to rename.

:cli:synopsis:`<new-name>`
    The new name of the branch.

:cli:synopsis:`--force`
    Close any existing connections to the branch before renaming it.
