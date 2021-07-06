.. _ref_cli_edgedb_self_install:


===================
edgedb self install
===================

Install the EdgeDB CLI tools and potentially set up the server.

.. cli:synopsis::

    edgedb self install [OPTIONS]


Description
===========

``edgedb self install`` is a terminal command used to install the CLI
tools themselves. This is basically what the EdgeDB :ref:`install
script <ref_admin_install>` will run. Alternatively, this is a way to
install EdgeDB if you've downloaded the binary manually.


Options
=======

:cli:synopsis:`--nightly`
    Install nightly version of command-line tools.

:cli:synopsis:`--no-modify-path`
    Do not configure the PATH environment variable.

:cli:synopsis:`--no-wait-for-exit-prompt`
    Indicate that the installation tool should not issue a "Press
    Enter to continue" prompt before exiting on Windows. This is for
    the cases where the script is invoked from an existing terminal
    session and not in a new window.

:cli:synopsis:`--no-modify-path`
    Do not configure the PATH environment variable.

:cli:synopsis:`-q, --quiet`
    Skip printing messages and confirmation prompts.

:cli:synopsis:`-v, --verbose`
    Verbose output.

:cli:synopsis:`-y`
    Disable confirmation prompt, also disables running
    :ref:`ref_cli_edgedb_project_init`.
