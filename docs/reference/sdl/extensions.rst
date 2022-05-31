.. _ref_eql_sdl_extensions:

==========
Extensions
==========

This section describes the SDL commands pertaining to
:ref:`extensions <ref_datamodel_extensions>`.


Syntax
------

Declare that the current schema enables a particular extension.

.. sdl:synopsis::

    using extension <ExtensionName> ";"


Description
-----------

Extension declaration must be outside any :ref:`module block
<ref_eql_sdl_modules>` since extensions affect the entire database and
not a specific module.


Examples
--------

Enable :ref:`GraphQL <ref_graphql_index>` extension for the current
schema:

.. code-block:: sdl

    using extension graphql;

Enable :ref:`EdgeQL over HTTP <ref_edgeql_http>` extension for the
current database:

.. code-block:: sdl

    using extension edgeql_http;
