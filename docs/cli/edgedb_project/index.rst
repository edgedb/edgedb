.. _ref_cli_edgedb_project:


==============
edgedb project
==============

EdgeDB provides a way to quickly setup a project. This way the project
directory gets associated with a specific EdgeDB instance and thus
makes it the default instance to connect to. This is done by creating
an :ref:`ref_reference_edgedb_toml` file in the project directory.

.. toctree::
    :maxdepth: 3
    :hidden:

    edgedb_project_init
    edgedb_project_info
    edgedb_project_unlink
    edgedb_project_upgrade

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_edgedb_project_init`
      - Initialize a new or existing project
    * - :ref:`ref_cli_edgedb_project_info`
      - Get various metadata about the project
    * - :ref:`ref_cli_edgedb_project_unlink`
      - Remove project association with an instance
    * - :ref:`ref_cli_edgedb_project_upgrade`
      - Upgrade EdgeDB instance used for the current project
