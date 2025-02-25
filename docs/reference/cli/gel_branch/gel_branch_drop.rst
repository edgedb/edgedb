.. _ref_cli_gel_branch_drop:


===============
gel branch drop
===============

Remove an existing :ref:`branch <ref_datamodel_branches>`.

.. cli:synopsis::

    gel branch drop [<options>] <name>

Options
=======

The ``branch drop`` command runs in the |Gel| instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<name>`
    The name of the branch to drop.

:cli:synopsis:`--non-interactive`
    Drop the branch without asking for confirmation.

:cli:synopsis:`--force`
    Close any existing connections to the branch before dropping it.
