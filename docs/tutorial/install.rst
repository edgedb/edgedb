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

    $ curl --proto '=https' --tlsv1.2 https://sh.edgedb.com -sSf | sh

The command downloads a script and starts the installation of the ``edgedb``
command line tool.  The script might require elevated privileges and might
ask you for your password.  Once the ``edgedb`` CLI installation is successful,
run the following command to configure your current shell to be able to
run ``edgedb`` commands (you only need to do this once):

.. code-block:: bash

    $ source ~/.edgedb/env

Then, let's install and configure the EdgeDB server:

.. code-block:: bash

    $ edgedb server install
    $ edgedb server init

With EdgeDB up and running we're ready to
:ref:`create a schema <ref_tutorial_createdb>`.
