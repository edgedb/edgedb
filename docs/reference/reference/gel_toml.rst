.. _ref_reference_gel_toml:

========
gel.toml
========

The |gel.toml| file is created in the project root after running
:ref:`ref_cli_gel_project_init`. If this file is present in a directory, it
signals to the CLI and client bindings that the directory is an instance-linked
|Gel| project. It supports the following configuration settings:

.. note::

    If you're not familiar with the TOML file format, it's a very cool, minimal
    language for config files designed to be simpler than JSON or YAML. Check
    out `the TOML documentation <https://toml.io/en/v1.0.0>`_.


[instance] table
================

- ``server-version``- The server version of the Gel project.

  .. note::

      The version specification is assumed to be **a minimum version**, but the
      CLI will *not* upgrade to subsequent major versions. This means if the
      version specified is ``6.1`` and versions 6.2 and 6.3 are available, 6.3
      will be installed, even if version 7.0 is also available.

      To specify an exact version, prepend with ``=`` like this: ``=6.1``. We
      support `all of the same version specifications as Cargo`_,
      Rust's package manager.

  .. note::

      ``edgedb.toml`` files for versions of |Gel| prior to 6.0 use
      ``[edgedb]`` table, not ``[instance]``.


[project] table
===============

- ``schema-dir``- The directory where schema files will be stored.
  Defaults to ``dbschema``.


[hooks] table
=============

.. versionadded:: 6

This table may contain the following keys, all of which are optional:

- ``project.init.before``
- ``project.init.after``
- ``branch.switch.before``
- ``branch.wipe.before``
- ``migration.apply.before``
- ``schema.update.before``
- ``branch.switch.after``
- ``branch.wipe.after``
- ``migration.apply.after``
- ``schema.update.after``

Each key represents a command hook that will be executed together with a CLI
command. All keys have a string value which is going to be executed as a shell
command when the corresponding hook is triggered.

Hooks are divided into two categories: ``before`` and ``after`` as indicated
by their names. All of the ``before`` hooks are executed prior to their
corresponding commands, so they happen before any changes are made. All of the
``after`` hooks run after the CLI command and thus the effects from the
command are already in place. Any error during the hook script execution will
terminate the CLI command (thus ``before`` hooks are able to prevent their
commands from executing if certain conditions are not met).

Overall, when multiple hooks are triggered they all execute sequentially in
the order they are listed above.

Here is a breakdown of which command trigger which hooks:

- :ref:`ref_cli_gel_project_init` command triggers the ``project.init.before``
  and ``project.init.after`` hook. If the migrations are applied at the end of
  the initialization, then the ``migration.apply.before``,
  ``schema.update.before``, ``migration.apply.after``, and
  ``schema.update.after`` hooks are also triggered.
- :ref:`ref_cli_gel_branch_switch` command triggers ``branch.switch.before``,
  ``schema.update.before``, ``branch.switch.after``, and ``schema.update.after``
  hooks in that relative order.
- :ref:`ref_cli_gel_branch_wipe` command triggers the ``branch.wipe.before``,
  ``schema.update.before``, ``branch.wipe.after``, and ``schema.update.after``
  hooks in that relative order.
- :ref:`ref_cli_gel_branch_rebase` and :ref:`ref_cli_gel_branch_merge`
  commands trigger ``migration.apply.before``, ``schema.update.before``,
  ``migration.apply.after``, and ``schema.update.after`` hooks in that
  relative order. Notice that although these are branch commands, but they do
  not change the current branch, instead they modify and apply migrations.
  That's why they trigger the ``migration.apply`` hooks.
- :ref:`ref_cli_gel_migration_apply` command triggers
  ``migration.apply.before``, ``schema.update.before``,
  ``migration.apply.after``, and ``schema.update.after`` hooks in that
  relative order.

  .. note::

    All of these hooks are intended as project management tools. For this
    reason they will only be triggered by the CLI commands that *don't
    override* default project settings. Any CLI command that uses
    :ref:`connection options <ref_cli_gel_connopts>` will not trigger any
    hooks.

This is implementing `RFC 1028 <rfc1028_>`_.

[[watch]] table array
=====================

.. versionadded:: 6

Each element of this table array may contain the following required keys:

- ``files = ["<path-string>", ...]`` - specify file(s) being watched.

  The paths must use ``/`` (\*nix-style) as path separators. They can also contain glob pattrens (``*``, ``**``, ``?``, etc.) in order to specify multiple files at one.

- ``script = "<command>"`` - command to be executed by the shell.

The watch mode can be activated by the :ref:`ref_cli_gel_watch` command.

This is implementing `RFC 1028 <rfc1028_>`_.


Example
=======

.. code-block:: toml

    [gel]
    server-version = "6.0"

    [project]
    schema-dir = "db/schema"

    [hooks]
    project.init.after="setup_dsn.sh"
    branch.wipe.after=""
    branch.switch.after="setup_dsn.sh"
    schema.update.after="gel-orm sqlalchemy --mod compat --out compat"

    [[watch]]
    files = ["queries/*.edgeql"]
    script = "npx @edgedb/generate queries"

.. lint-off

.. _all of the same version specifications as Cargo:
   https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies

.. _rfc1028:
    https://github.com/edgedb/rfcs/blob/master/text/1028-cli-hooks.rst

.. lint-on
