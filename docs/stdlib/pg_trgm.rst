.. versionadded:: 4.0

.. _ref_ext_pgtrgm:

============
ext::pg_trgm
============

This extension provides tools for your hashing and encrypting needs.

The Postgres that comes packaged with the EdgeDB 4.0+ server includes
``pg_trgm``, as does EdgeDB Cloud. It you are using a separate
Postgres backend, you will need to arrange for it to be installed.

To activate this functionality you can use the :ref:`extension
<ref_datamodel_extensions>` mechanism:

.. code-block:: sdl

    using extension pg_trgm;

That will give you access to the ``ext::pg_trgm`` module where you may find
the following functions:
