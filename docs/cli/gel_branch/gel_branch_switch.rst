.. _ref_cli_gel_branch_switch:


=================
gel branch switch
=================

Change the currently active :ref:`branch <ref_datamodel_branches>`

.. cli:synopsis::

    gel branch switch [<options>] <name>

.. note::

    This CLI command requires |Gel| (or |EdgeDB| version 5.0 or later.)
    Earlier versions did not feature branches and instead featured
    **databases**.

    Databases offered no direct analog to switching.

    - To run a single command on a different |branch|, use the ``-d <dbname>``
      or ``--database=<dbname>`` options described in
      :ref:`ref_cli_gel_connopts`
    - To change the database for *all* commands, set the :gelenv:`DATABASE`
      environment variable described in :ref:`ref_cli_gel_connopts`
    - To change the database for all commands in a project, you may update the
      ``credentials.json`` file's ``database`` value. To find that file for
      your project, run :ref:`ref_cli_gel_info` to get the config path and
      navigate to ``/<config-path>/credentials``.
    - You may use ``\connect <dbname>`` or ``\c <dbname>`` to change the
      connected database while in a REPL session.

    See the :ref:`ref_cli_gel_database` command suite for other database
    management commands.


Options
=======

The ``branch switch`` command runs in the |Gel| instance it is
connected to. For specifying the connection target see
:ref:`connection options <ref_cli_gel_connopts>`.

:cli:synopsis:`<name>`
    The name of the new |branch|.

:cli:synopsis:`-c, --create`
    Create the branch if it doesn't exist.

:cli:synopsis:`-e, --empty`
    If creating a new branch: create the branch with no schema or data.

:cli:synopsis:`--from <FROM>`
    If creating a new branch: the optional base branch to create the new branch
    from.

:cli:synopsis:`--copy-data`
    If creating a new branch: copy data from the base branch to the new branch.
