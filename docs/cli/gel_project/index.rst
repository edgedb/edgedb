.. _ref_cli_gel_project:


===========
gel project
===========

|Gel| provides a way to quickly setup a project. This way the project
directory gets associated with a specific Gel instance and thus
makes it the default instance to connect to. This is done by creating
an :ref:`ref_reference_gel_toml` file in the project directory.

.. toctree::
    :maxdepth: 3
    :hidden:

    gel_project_init
    gel_project_info
    gel_project_unlink
    gel_project_upgrade

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_gel_project_init`
      - Initialize a new or existing project
    * - :ref:`ref_cli_gel_project_info`
      - Get various metadata about the project
    * - :ref:`ref_cli_gel_project_unlink`
      - Remove project association with an instance
    * - :ref:`ref_cli_gel_project_upgrade`
      - Upgrade |Gel| instance used for the current project
