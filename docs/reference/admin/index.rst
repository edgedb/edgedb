.. _ref_admin:

Administration
==============

Administrative commands for managing Gel:


* :ref:`configure <ref_eql_statements_configure>`

  Configure server behavior.

* :ref:`database <ref_admin_databases>`

  Create or remove a database.

* :ref:`role <ref_admin_roles>`

  Create, remove, or alter a role.

.. versionadded:: 5.0

    New administrative commands were added in |EdgeDB| 5 release:

    * :ref:`branch <ref_admin_branches>`

      Create, remove, or alter a branch.

    * :ref:`administer statistics_update() <ref_admin_statistics_update>`

      Update internal statistics about data.

    * :ref:`administer vacuum() <ref_admin_vacuum>`

      Reclaim storage space.


.. toctree::
    :maxdepth: 3
    :hidden:

    configure
    databases
    roles
    statistics_update
    vacuum
