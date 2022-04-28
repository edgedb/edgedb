.. _ref_admin_install:

============
Installation
============

Below are instructions for installing the CLI, installed EdgeDB itself, and
managing installing.

Install the CLI
---------------

To get started with EdgeDB, the first step is install the ``edgedb`` CLI.

**Linux or macOS**

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

**Windows Powershell**

.. code-block:: powershell

    PS> iwr https://ps1.edgedb.com -useb | iex

Follow the prompts on screen to complete the installation. The script will
download the ``edgedb`` command built for your OS and add a path to it to your
shell environment. To test the installation, run ``edgedb --version`` from the
command line.


.. code-block:: bash

    $ edgedb --version
    EdgeDB CLI 1.x+abcdefg


If you encounter a ``command not found`` error, you may need to open a new
terminal window before the ``edgedb`` command is available.


.. note::

    To install the CLI with a package manager, refer to the "Additional
    methods" section of the `Install <https://www.edgedb.com/install>`_ page
    for instructions.


Installing EdgeDB
-----------------

Once the CLI is installed, you can spin up local instances. The latest version
of EdgeDB will be automatically installed whenever you create a new instance.

.. code-block:: bash

    $ edgedb instance create my_instance
    Downloading package...
    00:00:00 [====================] 34.03MiB/34.03MiB 45.41MiB/s | ETA: 0s
    Successfully installed 1.x+abcdefg
    Initializing EdgeDB instance...
    Instance my_instance is up and running.
    To connect to the instance run:
      edgedb -I my_instance

You don't need to worry about manually managing EdgeDB installations; just
create instances as you need them; EdgeDB will automatically install the
latest minor version if you don't already have it installed.

Once you have the CLI installed, we recommend checking out
the :ref:`Quickstart <ref_quickstart>` guide.

Nightly installations
^^^^^^^^^^^^^^^^^^^^^

You can also install a nightly build of EdgeDB with the ``nightly`` flag.

.. code-block:: bash

    $ edgedb instance create my_instance --nightly
    Downloading package...
    00:00:00 [====================] 34.03MiB/34.03MiB 45.41MiB/s | ETA: 0s
    Successfully installed 2.0-dev.6521+1886f44
    Initializing EdgeDB instance...
    Instance my_instance is up and running.

This installs the nightly build of the upcoming *major version*
(currently 2.0).

View all installed versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To view all versions of EdgeDB that exist and their installation status:

.. code-block:: bash

    $ edgedb server list-versions
    ┌─────────┬──────────────────────┬───────────┐
    │ Channel │ Version              │ Installed │
    │ stable  │ 1.0+9236fa8          │           │
    │ stable  │ 1.1+ab7d5a1          │ ✓         │
    │ stable  │ 1.2+0f7e719          │ ✓         │
    │ stable  │ 1.3+804c096          │ ✓         │
    │ nightly │ 2.0-dev.6355+e7dd871 │ ✓         │
    └─────────┴──────────────────────┴───────────┘

Uninstall
---------

To uninstall a particular version, pass the ``Version`` tag from the table
above into the following command.

.. code-block:: bash

    $ edgedb server uninstall --version 1.1+ab7d5a1
    Successfully uninstalled 1 versions.

Or uninstall several versions at once with the following helper flags.

.. code-block::

    --all        Uninstall all versions
    --nightly    Uninstall nightly versions
    --unused     Uninstall unused versions

View the :ref:`edgedb server <ref_cli_edgedb_server>` CLI reference for a
comprehensive reference to installation management.
