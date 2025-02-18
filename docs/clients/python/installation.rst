.. _gel-python-installation:


Installation
============

The recommended way to install the |Gel| driver is to use **pip**:

.. code-block:: bash

    $ pip install gel


.. note::

   It is recommended to use **pip** version **8.1** or later to take
   advantage of the precompiled wheel packages.  Older versions of pip
   will ignore the wheel packages and install from the source
   package.  In that case a working C compiler is required.


Building from source
--------------------

If you want to build the |Gel| driver from a Git checkout you will need:

* A working C compiler.
* CPython header files.  These can usually be obtained by installing
  the relevant Python development package: **python3-dev** on Debian/Ubuntu,
  **python3-devel** on RHEL/Fedora.

Once the above requirements are satisfied, run the following command
in the root of the source checkout:

.. code-block:: bash

    $ pip install -e .


Running tests
-------------

The testsuite requires a working local installation of the Gel server.
To execute the testsuite run:

.. code-block:: bash

    $ python setup.py test
