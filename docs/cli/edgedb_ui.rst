.. _ref_cli_edgedb_ui:


=========
edgedb ui
=========

Open the EdgeDB UI of the current instance in your default browser.

.. cli:synopsis::

    edgedb ui [<options>]


Description
===========

``edgedb ui`` is a terminal command used to open the EdgeDB UI in your default
browser. Alternatively, it can be used to print the UI URL with the
``--print-url`` option.

The EdgeDB UI is a tool that allows you to graphically manage and query your
EdgeDB databases. It contains a REPL, a textual and graphical view of your
database schemas, and a data explorer which allows for viewing your data as a
table.

.. note::

    The UI is served by default by development instances. To enable the UI on a
    production instance, use the ``--admin-ui`` option with ``edgedb-server``
    or set the ``EDGEDB_SERVER_ADMIN_UI`` :ref:`environment variable
    <ref_reference_envvar_admin_ui>` to ``enabled``.


Options
=======

The ``ui`` command runs on the database it is connected to. For specifying the
connection target see :ref:`connection options <ref_cli_edgedb_connopts>`.

:cli:synopsis:`--print-url`
    Print URL in console instead of opening in the browser. This is useful if
    you prefer to open the EdgeDB UI in a browser other than your default
    browser.

:cli:synopsis:`--no-server-check`
    Skip probing the UI endpoint of the server instance. The endpoint probe is
    in place to provide a friendly error if you try to connect to a UI on a
    remote instance that does not have the UI enabled.

Media
=====

.. edb:youtube-embed:: iwnP_6tkKgc