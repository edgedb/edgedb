:orphan:

.. _ref_extvectorstore_reference:

================
ext::vectorstore
================


The ``ext::vectorstore`` extension package provides simplified vectorstore
workflows for |Gel|, built on top of the pgvector integration. It includes
predefined vector dimensions and a base schema for vector storage records.


Enabling the extension
======================

The extension package can be installed using the :gelcmd:`extension` CLI
command:

.. code-block:: bash

    $ gel extension install vectorstore


It can be enabled using the :ref:`extension <ref_datamodel_extensions>`
mechanism:

.. code-block:: sdl

    using extension vectorstore;


The Vectorstore extension is designed to be used in combination with the
:ref:`Vectostore Python binding <ref_ai_vectorstore_python>` or other
integrations, rather than on its own.


Types
=====

Vector Types
------------

The extension provides two pre-defined vector types with different dimensions:

- ``ext::vectorstore::vector_1024``: 1024-dimensional vector
- ``ext::vectorstore::vector_1536``: 1536-dimensional vector

All vector types extend ``ext::pgvector::vector`` with their respective dimensions.


Record Types
------------

.. eql:type:: ext::vectorstore::BaseRecord

    Abstract type that defines the basic structure for vector storage records.

    Properties:

    * ``collection: str`` (required): Identifies the collection the record belongs to
    * ``text: str``: Associated text content
    * ``embedding: ext::pgvector::vector``: The vector embedding
    * ``external_id: str``: External identifier with unique constraint
    * ``metadata: json``: Additional metadata in JSON format


.. eql:type:: ext::vectorstore::DefaultRecord

    Extends :eql:type:`ext::vectorstore::BaseRecord` with specific
    configurations.

    Properties:

    * Inherits all properties from :eql:type:`ext::vectorstore::BaseRecord`
    * Specializes ``embedding`` to use ``vector_1536`` type
    * Includes an HNSW cosine similarity index on the embedding with:

      * ``m = 16``
      * ``ef_construction = 128``


