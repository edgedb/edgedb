.. _ref_cli_gel_branch_wipe:


===============
gel branch wipe
===============

Destroy the contents of a :ref:`branch <ref_datamodel_branches>`

.. cli:synopsis::

    gel branch wipe [<options>] <name>

Description
===========

The contents of the branch will be destroyed and the schema reset to its
state before any migrations, but the branch itself will be preserved.

:gelcmd:`branch wipe` is a terminal command equivalent to
:eql:stmt:`reset schema to initial`.


Options
=======

The ``branch wipe`` command runs in the |Gel| instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<name>`
    The name of the branch to wipe.

:cli:synopsis:`--non-interactive`
    Destroy the data without asking for confirmation.
