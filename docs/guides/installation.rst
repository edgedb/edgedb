.. _ref_admin_install:

============
Installation
============

To install EdgeDB, first install EdgeDB's command line tool.

**Linux or macOS**

.. code-block:: bash

    $ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh

<<<<<<< HEAD
If you are using Windows the command is different:

.. code-block:: powershell
=======
**Windows Powershell**

.. code-block:: bash

    $ iwr https://ps1.edgedb.com -useb | iex

.. note::

    The `Install page <edgedb.com/install>`Alternatively, you can install ``edgedb-cli`` using a supported package
manager as described on the `Downloads <https://www.edgedb.com/download/>`_
page under the "Other Installation Options" section.
>>>>>>> 1528a9954 (Update read commands)

    PS> iwr https://ps1.edgedb.com -useb | iex

The script will download the ``edgedb`` command built for your OS and add
a path to it to your shell environment. Follow the script instructions to
complete the CLI installation.

Alternatively, you can install ``edgedb-cli`` using a supported package
manager as described on the `Downloads <https://www.edgedb.com/download/>`_
page under the "Other Installation Options" section.

Once you have the CLI installed on your computer you can check out
our :ref:`quickstart <ref_quickstart>` guide!
