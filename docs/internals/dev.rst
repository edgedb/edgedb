=================
Developing EdgeDB
=================

This section describes how to build EdgeDB locally, how to use its
internal tools, and how to contribute to it.


Building Locally
================

The following instructions should be used to create a "dev" build on
Linux or macOS.  Windows is not currently supported.

.. rubric:: Build Requirements

* GNU make version 3.80 or newer;
* C compiler (GCC or clang);
* autotools;
* Python 3.7 dev package;
* Bison 1.875 or later;
* Flex 2.5.31 or later;
* Perl 5.8.3 or later;
* Zlib (zlibg1-dev on Ubuntu);
* Readline dev package;
* Libuuid dev package.

.. zlib, readline and libuuid are required to build postgres. Should be removed
   when custom postgres build is no longer needed.


.. rubric:: Instructions

The easiest way to set up a development environment is to create a
Python "venv" with all dependencies and commands installed into it.

#. Make a new directory that will contain checkouts of `edgedb <edgedb_>`_
   and `edgedb-python <edgedbpy_>`_.  The name of the directory is
   arbitrary, we will use "dev" in this guide:

   .. code-block:: bash

      $ mkdir ~/dev
      $ cd ~/dev

#. Clone edgedb and edgedb-python repositories:

   .. code-block:: bash

      $ git clone --recursive git@github.com:edgedb/edgedb.git
      $ git clone --recursive git@github.com:edgedb/edgedb-python.git

#. Create a Python 3.7 virtual environment and activate it:

   .. code-block:: bash

      $ python3.7 -m venv edgedb-dev
      $ source edgedb-dev/bin/activate

#. Build edgedb-python:

   .. code-block:: bash

      $ cd edgedb-python
      $ pip install -v -e .

#. Build edgedb (the build will take a while):

   .. code-block:: bash

      $ cd ../edgedb
      $ pip install -v -e ".[test,docs]"

   In addition to compiling EdgeDB and all dependencies, this will also
   install ``edb`` and ``edgedb`` command line tools into the current
   Python virtual environment.

   It will also install libraries used during development.

#. Run tests:

   .. code-block:: bash

      $ edb test

The new virtual environment is now ready for development and can be
activated at any time.


Running Tests
=============

To run all EdgeDB tests simply use the ``$ edb test`` command without
arguments.

The command also supports running a few selected tests.  To run all
tests in a test case file:

.. code-block:: bash

   $ edb test tests/test_edgeql_calls.py

   # or run two files:
   $ edb test tests/test_edgeql_calls.py tests/test_edgeql_for.py

To pattern-match a test but its name:

.. code-block:: bash

   $ edb test -k test_edgeql_calls_01

   # or run all tests that contain "test_edgeql_calls":
   $ edb test -k test_edgeql_calls

See ``$ edb test --help`` for more options.


Dev Server
==========

Use the ``$ edb server`` command to start the development server.

You can then use another terminal to open a REPL to the server using the
``$ edgedb`` command, or connect to it using one of the language bindings.


Test Databases
==============

Use the ``$ edb inittestdb`` command to create and populate databases
that are used by unit tests.


.. _edgedbpy: https://github.com/edgedb/edgedb-python
.. _edgedb: https://github.com/edgedb/edgedb
