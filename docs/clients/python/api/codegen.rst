.. _edgedb-python-codegen:

===============
Code Generation
===============

.. py:currentmodule:: edgedb

The ``edgedb-python`` package exposes a command-line tool to generate
typesafe functions from ``*.edgeql`` files, using :py:mod:`dataclasses` for
objects primarily.

.. code-block:: bash

  $ edgedb-py

Or alternatively:

.. code-block:: bash

  $ python -m edgedb.codegen

Consider a simple query that lives in a file called ``get_number.edgeql``:

.. code-block:: edgeql

  select <int64>$arg;

Running the code generator will generate a new file called
``get_number_async_edgeql.py`` containing the following code (roughly):

.. code-block:: python

  from __future__ import annotations
  import edgedb


  async def get_number(
      client: edgedb.AsyncIOClient,
      *,
      arg: int,
  ) -> int:
      return await client.query_single(
          """\
          select <int64>$arg\
          """,
          arg=arg,
      )

Target
~~~~~~

By default, the generated code uses an ``async`` API. The generator supports
additional targets via the ``--target`` flag.

.. code-block:: bash

  $ edgedb-py --target async        # generate async function (default)
  $ edgedb-py --target blocking     # generate blocking code

The names of the generated files will differ accordingly:
``{query_filename}_{target}_edgeql.py``.

Single-file mode
~~~~~~~~~~~~~~~~

It may be preferable to generate a single file containing all the generated
functions. This can be done by passing the ``--file`` flag.

.. code-block:: bash

  $ edgedb-py --file

This generates a single file called ``generated_{target}_edgeql.py`` in the
root of your project.

Connection
~~~~~~~~~~

The ``edgedb-py`` command supports the same set of :ref:`connection options
<ref_cli_edgedb_connopts>` as the ``edgedb`` CLI.

.. code-block::

    -I, --instance <instance>
    --dsn <dsn>
    --credentials-file <path/to/credentials.json>
    -H, --host <host>
    -P, --port <port>
    -d, --database <database>
    -u, --user <user>
    --password
    --password-from-stdin
    --tls-ca-file <path/to/certificate>
    --tls-security <insecure | no_host_verification | strict | default>

