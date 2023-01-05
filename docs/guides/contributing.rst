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
* Rust compiler and Cargo 1.59 or later;
* autotools;
* Python 3.10 dev package;
* Bison 1.875 or later;
* Flex 2.5.31 or later;
* Perl 5.8.3 or later;
* Zlib (zlibg1-dev on Ubuntu);
* Readline dev package;
* Libuuid dev package;
* Node.js 14 or later;
* Yarn 1

.. zlib, readline and libuuid are required to build postgres. Should be removed
   when custom postgres build is no longer needed.

On Ubuntu 22.10, these can be installed by running:

.. code-block:: bash

   $ apt install make gcc rust-all autotools-dev python3.11-dev \
     python3.11-venv bison flex libreadline-dev perl zlib1g-dev \
     uuid-dev nodejs npm
   $ npm i -g corepack
   $ corepack enable && corepack prepare yarn@stable --activate

.. rubric:: Instructions

The easiest way to set up a development environment is to create a
Python "venv" with all dependencies and commands installed into it.

#. Make a new directory that will contain checkouts of `edgedb <edgedb_>`_
   and `edgedb-python <edgedbpy_>`_.  The name of the directory is
   arbitrary, we will use "dev" in this guide:

   .. code-block:: bash

      $ mkdir ~/dev
      $ cd ~/dev

#. Clone the edgedb repository using ``--recursive``
   to clone all submodules:

   .. code-block:: bash

      $ git clone --recursive https://github.com/edgedb/edgedb.git

#. Create a Python 3.10 virtual environment and activate it:

   .. code-block:: bash

      $ python3.10 -m venv edgedb-dev
      $ source edgedb-dev/bin/activate

#. Build edgedb (the build will take a while):

   .. code-block:: bash

      $ cd edgedb
      $ pip install -v -e ".[test]"

   In addition to compiling EdgeDB and all dependencies, this will also
   install the ``edb`` and ``edgedb`` command line tools into the current
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

To pattern-match a test by its name:

.. code-block:: bash

   $ edb test -k test_edgeql_calls_01

   # or run all tests that contain "test_edgeql_calls":
   $ edb test -k test_edgeql_calls

See ``$ edb test --help`` for more options.


Writing Documentation
=====================

The ``edgedb`` repository contains all of its documentation in the ``docs/``
directory. EdgeDB uses `reStructuredText with Sphinx <rst_>`_.

Use the ``$ make docs`` command to build and generate HTML files from the
documentation. The repository contains a ``Makefile`` for all of Sphinx's
necessary build options.

Upon success, HTML generated documentation will be a new directory path
as ``docs/build``.

Dev Server
==========

Use the ``$ edb server`` command to start the development server.

You can then use another terminal to open a REPL to the server using the
``$ edgedb`` command, or connect to it using one of the language bindings.


Test Databases
==============

Use the ``$ edb inittestdb`` command to create and populate databases
that are used by unit tests.

.. _rst: https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html
.. _edgedbpy: https://github.com/edgedb/edgedb-python
.. _edgedb: https://github.com/edgedb/edgedb
