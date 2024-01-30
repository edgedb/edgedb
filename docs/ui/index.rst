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
the REPL, the editor, and the data explorer. Which one to use can generally
be decided based on your use case.

The *data explorer* offers simple point-and-click access to objects without
needing any EdgeQL, making it the recommended for:

- Outright new users to EdgeDB who lack a technical background or the time
  to familiarize themself with EdgeQL,
- Existing users of EdgeDB looking to "walk" the database's objects without
  needing to construct a new query each time.
- Users with a desire to double- or triple-check inserts, updates and
  deletions, as as the data explorer will first collect and display all user
  changes in a double-confirm dialog before allowing an operation to proceed.

The Editor page's *query builder* is recommended for:

- Users who are learning EdgeQL but still lacking the muscle memory to compose
  queries on the fly,
- Users querying objects with a large number of properties or links, as the
  query builder displays all properties and links by default. This makes
  visualizing an object's structure easier compared to using a command like
  ``describe type <TypeName>`` to see its internals.

The Editor page's *query editor* is recommended for:

- Users experimenting with various raw queries who want quick visual
  point-and-click access to past queries in order to call them up again
  and refine them.

The UI's REPL is recommended for:

- Users comfortable with EdgeQL.

Additionally, users who spend a lot of time comparing raw queries may also
wish to give the CLI's REPL a try. A general rule of thumb is that the
UI's REPL provides a more slicker experience and more verbose output, while
the CLI's REPL is a more performant tool that usually returns query results
instantaneously.

For example, the output from an object type in the UI's REPL will show more
information on a scalar object's type name:

.. code-block::

  # CLI REPL output
  default::Sailor {
    id: f0b4aaf0-be4c-11ee-b84b-6b87ec260333,
    cents: 0,
    dollars: 0,
    pence: 149,
    pounds: 14,
    shillings: 28,
    total_cents: 0,
    total_pence: 4069,
    approx_wealth_in_pounds: 17,
  },

  # UI REPL output
  default::Sailor {
    id: <uuid>'f0b4aaf0-be4c-11ee-b84b-6b87ec260333',
    cents: <default::Money>0,
    dollars: <default::Money>0,
    pence: <default::Money>149,
    pounds: <default::Money>14,
    shillings: <default::Money>28,
    total_cents: 0,
    total_pence: 4069,
    approx_wealth_in_pounds: 17,
  },

One more example of CLI vs. UI output, showing a user-defined function:

.. code-block::

  # CLI REPL output
  'function default::get_url() ->  std::str using
  (<std::str>\'https://geohack.toolforge.org/geohack.php?params=\');'}

  # UI REPL output
  function default::get_url() -> std::str {
  volatility := 'Immutable';
  using (<std::str>'https://geohack.toolforge.org/geohack.php?params=');
  }

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