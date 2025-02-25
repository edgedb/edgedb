.. _ref_cli_gel_watch:


=========
gel watch
=========

Start a long-running process that watches for changes as specified in the
:ref:`gel.toml <ref_reference_gel_toml>` file. This process will monitor the
project for changes specified in the ``[[watch]]`` table array and run the
associated scripts in response to those changes.

When multiple changes target the same ``[[watch]]`` element, the corresponding
script will be triggered only once. All triggered watch scripts will be
executed in parallel. If the same script is triggered before it finishes
executing, the next execution will wait for the already running script to
terminate (i.e. only one instance of the same script will be runing at the
same time).

.. note::

    Any output that the triggered scripts produce will be shown in the
    :gelcmd:`watch` console. This includes any error messages. So if you're
    not seeing a change you've expected, check on the watch process to make
    sure there aren't any unexpected errors in the triggered scripts.

To learn about our recommended development migration workflow using
:gelcmd:`watch`, read our :ref:`intro to migrations <ref_intro_migrations>`.

Options
=======

.. warning::

    This command changed in version 6. In older versions it only monitored the
    schema file changes and it had no additional options.

.. versionadded:: 6.0

:cli:synopsis:`--migrate`
    Watches for changes in schema files in your project's ``dbschema``
    directory and applies those changes to your current |branch| in real time.

    If a schema change cannot be applied, you will see an error in the
    :gelcmd:`watch` console. You will also receive the error when you
    try to run a query with any |Gel| client binding.

    .. note::

        If you want to apply a migration in the same manner as ``watch
        --migrate`` but without the long-running process, use :gelcmd:`migrate
        --dev-mode`. See :ref:`ref_cli_gel_migration_apply` for more details.

:cli:synopsis:`-v, --verbose`
    Verbose output.
