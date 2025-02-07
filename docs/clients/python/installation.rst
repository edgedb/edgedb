.. _edgedb-python-installation:


Installation
============

The recommended way to install the EdgeDB driver is to use **pip**:

.. code-block:: bash

    $ pip install edgedb


.. note::

   It is recommended to use **pip** version **8.1** or later to take
   advantage of the precompiled wheel packages.  Older versions of pip
   will ignore the wheel packages and install from the source
   package.  In that case a working C compiler is required.


Building from source
--------------------

If you want to build the EdgeDB driver from a Git checkout you will need:

* A working C compiler.
* CPython header files.  These can usually be obtained by installing
  the relevant Python development package: **python3-dev** on Debian/Ubuntu,
  **python3-devel** on RHEL/Fedora.

Once the above requirements are satisfied, run the following command
in the root of the source checkout:

.. code-block:: bash

    $ pip install -e .

A debug build containing more runtime checks at the expense of performance
can be created by setting the ``EDGEDB_DEBUG`` environment variable when
building:

.. code-block:: bash

    $ env EDGEDB_DEBUG=1 pip install -e .


Running tests
-------------

The testsuite requires a working local installation of the EdgeDB server.
To execute the testsuite run:

.. code-block:: bash

    $ python setup.py test
