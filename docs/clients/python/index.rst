.. _edgedb-python-intro:

====================
EdgeDB Python Driver
====================

**edgedb-python** is the official EdgeDB driver for Python.
It provides both :ref:`blocking IO <edgedb-python-blocking-api-reference>`
and :ref:`asyncio <edgedb-python-asyncio-api-reference>` implementations.

.. rubric:: Contents

* :ref:`edgedb-python-installation`

  edgedb-python is installable via ``$ pip install edgedb``.  Read
  the section for more information on how to install the library.

* :ref:`edgedb-python-examples`

  High-level examples on how to use blocking and asyncio connections,
  as well as on how to work with transactions.

* :ref:`edgedb-python-asyncio-api-reference`

  Asynchronous API reference.

* :ref:`edgedb-python-blocking-api-reference`

  Synchronous API reference.

* :ref:`edgedb-python-datatypes`

  EdgeDB Python types documentation.

* :ref:`edgedb-python-codegen`

  Python code generation command-line tool documentation.
  
* :ref:`edgedb-python-advanced`

  Advanced usages of the state and optional customization.


.. toctree::
   :maxdepth: 3
   :hidden:

   installation
   usage
   api/asyncio_client
   api/blocking_client
   api/types
   api/codegen
   api/advanced
