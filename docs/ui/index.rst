.. _ref_ui_overview:

==
UI
==

EdgeDB UI is a beautiful, feature-rich admin panel baked directly into all 
EdgeDB instances.

The UI automatically opens in your default browser on a ``localhost`` page
via a single command:

.. code-block:: bash

  $ edgedb ui

Alternatively, you can print a url to paste into the browser instead of
automatically opening the UI:

.. code-block:: bash

  $ edgedb ui --print-url

The UI interface for :ref:`EdgeDB Cloud <ref_cli_edgedb_cloud>` is nearly
identical to the standard EdgeDB UI, aside from extra pages on cloud-related
items such as org settings, billing, and usage metrics.

The command to open the EdgeDB UI is a CLI command, documented
:ref:`here <ref_cli_edgedb_ui>`.

The UI is served by default by development instances. To enable the UI on a
production instance, use the ``--admin-ui`` option with ``edgedb-server``
or set the ``EDGEDB_SERVER_ADMIN_UI`` :ref:`environment variable
<ref_reference_envvar_admin_ui>` to ``enabled``.

The UI has three similar pages that each allow users to query the database:
the REPL, the editor, and the data explorer.

Outright new users to EdgeDB who need to query or work with objects in a
database will find the data explorer tab the easiest to use, as it allows
simple point-and-click access to objects without using any EdgeQL. The next
easiest page to use is the query builder inside the Editor tab, which does
use EdgeQL but is also point-and-click as it walks users through every step
of a query. The REPL is the most advanced tab in the UI as it requires the
user to be able to compose queries in EdgeQL.

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