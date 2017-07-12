.. toctree::
    :hidden:

    codestyle


.. _dev_index:


======================
Contributing to EdgeDB
======================

Prerequisites
=============

To work on EdgeDB you would need at least the following installed on
your system:

- CPython 3.6.2 or later
- PostgreSQL 9.6.3 or later


Installation and Build
======================

1. Obtain EdgeDB source code::

    $ git clone https://github.com/edgedb/edgedb.git

2. Create and activate a Python virtual environment::

    $ python3 -m venv edbenv
    $ source edbenv/bin/activate
    (edbenv) $

3. Install Python dependencies::

    (edbenv) $ cd edgedb
    (edbenv) $ pip install -r requirements.txt && pip install -e .


Running Tests
=============

To run the entire test suite, simply run ``pytest`` from the root
project directory.  ``pytest`` also supports selective test running
via the ``-k`` command line option.

It is also possible to run tests with standard ``unittest``::

    $ python3 setup.py test


Development Instance
====================

To create a development instance of EdgeDB, run
``edgedb-server -D <dir> [-p <port>]``, where ``<dir>`` is the
database instance data directory.

When working on a new test, or fixing an existing test failure, it is
recommended to use the ``tests.initlocal`` script to create a database
instance and populate it with schemas and data from tests::

    $ python3 tests/initlocal.py -D <data-dir>


Code Style
==========

See :ref:`dev_codestyle`.
