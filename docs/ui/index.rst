.. _ref_ui_overview:

==
UI
==

EdgeDB UI is a beautiful, feature-rich admin panel baked directly into all 
EdgeDB 2.0+ instances.

The UI automatically opens in your default browser on a ``localhost`` page
via a single command:

.. code-block:: bash

  $ edgedb ui

Alternatively, you can print a url to paste into the browser instead of
automatically opening the UI:

.. code-block:: bash

  $ edgedb ui --print-url

The command to open the EdgeDB UI is a CLI command, documented
:ref:`here <ref_cli_edgedb_ui>`.

The UI is served by default by development instances. To enable the UI on a
production instance, use the ``--admin-ui`` option with ``edgedb-server``
or set the ``EDGEDB_SERVER_ADMIN_UI`` :ref:`environment variable
<ref_reference_envvar_admin_ui>` to ``enabled``.

.. toctree::
  :maxdepth: 1

  ui_home
  database_dashboard
  client_settings
  ui_repl
  editor
  schema_viewer
  data_explorer
  auth_admin

`UI souce code <https://github.com/edgedb/edgedb-ui>`_