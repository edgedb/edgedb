.. _ref_cli_gel_branch_rebase:


=================
gel branch rebase
=================

Create a :ref:`branch <ref_datamodel_branches>` based on the target branch but
including new migrations on the current branch.

.. cli:synopsis::

    gel branch rebase [<options>] <name>


Description
===========

Creates a new branch that is based on the target branch, but also contains any new migrations on the
current branch.

.. note::

    When rebasing, the data of the target branch is preserved. This means that
    if you switch to a branch ``feature`` and run :gelcmd:`branch rebase
    main`, you will end up with a branch with the schema from |main| and any
    new migrations from ``feature`` and the data from |main|.

For more about how rebasing works, check out the breakdown :ref:`in our schema
migrations guide <ref_migration_guide_branches_rebasing>`.


Options
=======

The ``branch rebase`` command runs in the |Gel| instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<name>`
    The name of the target branch.
