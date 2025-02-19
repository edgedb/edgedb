.. _ref_reference_gel_toml:

========
gel.toml
========

The |gel.toml| file is created in the project root after running
:ref:`ref_cli_gel_project_init`. If this file is present in a directory, it
signals to the CLI and client bindings that the directory is an instance-linked
|Gel| project. It supports two configuration settings across two tables:

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


Example
=======

.. code-block:: toml

    [gel]
    server-version = "6.0"

    [project]
    schema-dir = "db/schema"

.. lint-off

.. _all of the same version specifications as Cargo:
   https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies

.. lint-on
