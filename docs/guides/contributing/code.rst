.. _ref_guide_contributing_code:

====
Code
====

:edb-alt-title: Developing EdgeDB

This section describes how to build EdgeDB locally, how to use its
internal tools, and how to contribute to it.

.. warning::

    Code-changing pull requests without adding new tests might take
    longer time to be reviewed and merged.

.. _ref_guide_contributing_code_build:

Building Locally
================

The following instructions should be used to create a "dev" build on
Linux or macOS.  Windows is not currently supported.

.. rubric:: Build Requirements

* GNU make version 3.80 or newer;
* C compiler (GCC or clang);
* Rust compiler and Cargo 1.80 or later;
* autotools;
* Python 3.12 dev package;
* Bison 1.875 or later;
* Flex 2.5.31 or later;
* Perl 5.8.3 or later;
* icu4c 4.6 or later;
* Zlib (zlibg1-dev on Ubuntu);
* Readline dev package;
* Libuuid dev package;
* Node.js 14 or later;
* Yarn 1
* Protobuf & C bindings for Protobuf

.. zlib, readline and libuuid are required to build postgres. Should be removed
   when custom postgres build is no longer needed.

On Ubuntu 24.04, these can be installed by running:

.. code-block:: bash

   $ apt install make gcc rust-all autotools-dev python3.12-dev \
     python3.12-venv bison flex libreadline-dev perl zlib1g-dev \
     uuid-dev nodejs npm
   $ npm i -g corepack
   $ corepack enable && corepack prepare yarn@stable --activate

On macOS, these can be installed by running:

.. code-block:: bash

   $ brew install rustup autoconf libtool python@3.12 readline zlib nodejs icu4c

To build Postgres on macOS, you'll need to set ``PKG_CONFIG_PATH`` so it can find
the ``icu4c`` libraries. This can be done manually each time you rebuild Postgres,
or set in your ``.profile`` or virtual environment.

.. code-block:: bash

   $ export PKG_CONFIG_PATH="$(brew --prefix icu4c)/lib/pkgconfig"

A Nix shell with all dependencies and a Python virtual environment can
be built with the following ``shell.nix`` file.

.. code::

   with import <nixpkgs> {};
   pkgs.mkShell {
       name = "edgedb dev shell";
       venvDir = "./venv";

       buildInputs = with pkgs; [
           python312Packages.python
           python312Packages.venvShellHook
           rustup
           autoconf
           automake
           bison
           flex
           perl
           zlib
           readline
           libuuid
           nodejs
           yarn
           openssl
           pkg-config
           icu
           protobuf
           protobufc
       ];
       LD_LIBRARY_PATH = lib.makeLibraryPath [ pkgs.stdenv.cc.cc ];
       LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

       # If you are using NixOS:
       # Postgres configure script uses /bin/pwd,
       # which does not exist on NixOS.
       #
       # I had a workaround for replacing /bin/pwd with pwd,
       # but it was annoying that postgres/ was dirty.
       # So my fix now is:
       # $ sudo sh -c "echo 'pwd' > /bin/pwd"
       # $ sudo chmod +x /bin/pwd
   }

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

#. Create a Python 3.12 virtual environment and activate it:

   .. code-block:: bash

      $ python3.12 -m venv edgedb-dev
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


Dev Server
==========

Use the ``$ edb server`` command to start the development server.

You can then use another terminal to open a REPL to the server using the
``$ edgedb`` command, or connect to it using one of the language bindings.


Test Branches
=============

Use the ``$ edb inittestdb`` command to create and populate branches that are
used by unit tests.

.. _rst: https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html
.. _edgedbpy: https://github.com/edgedb/edgedb-python
.. _edgedb: https://github.com/edgedb/edgedb
