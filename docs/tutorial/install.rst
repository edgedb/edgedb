.. _ref_tutorial_install:

1. Installation
===============

.. NOTE this is a good place to mention sublime, atom, vs code and vim
..      extensions for EdgeDB

The first step is to install the EdgeDB command line tools.  The easiest
way to do it is to run the installer as shown below.

If you are using Linux or macOS, open a terminal and enter the following
command:

.. code-block:: bash

    $ curl https://sh.edgedb.com --proto '=https' -sSf1 | sh

On Windows, open a PowerShell terminal and enter the following:

.. code-block:: powershell

    PS> iwr https://ps1.edgedb.com -useb | iex

The command downloads a script and starts the installation of the ``edgedb``
command line tool.  The script might require elevated privileges and might
ask you for your password.

Once the ``edgedb`` CLI installation is successful,
you might need to restart your terminal to be able to run ``edgedb`` commands.

Now, let's install the EdgeDB server component:

.. code-block:: bash

    $ edgedb server install

Depending on your OS, the native server packages might not yet be available,
and you might need to install and run Docker to complete the EdgeDB server
installation.

We are now ready to
:ref:`create a new database instance <ref_tutorial_createdb>`.
