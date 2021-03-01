.. _ref_eql_ddl_extensions:

==========
Extensions
==========

This section describes the DDL commands pertaining to
:ref:`extensions <ref_datamodel_extensions>`.


CREATE EXTENSION
================

:eql-statement:

Enable a particular extension for the current schema.

.. eql:synopsis::

    CREATE EXTENSION <ExtensionName> ";"

There's a :ref:`corresponding SDL declaration
<ref_eql_sdl_extensions>` for enabling an extension, which is the
recommended way of doing this.

Description
-----------

``CREATE EXTENSION`` enables the specified extension for the current database.

Examples
--------

Enable :ref:`GraphQL <ref_graphql_index>` extension for the current
schema:

.. code-block:: edgeql

    CREATE EXTENSION graphql;

Enable :ref:`EdgeQL over HTTP <ref_edgeql_index>` extension for the
current database:

.. code-block:: edgeql

    CREATE EXTENSION edgeql_http;


DROP EXTENSION
==============

:eql-statement:


Disable an extension.

.. eql:synopsis::

    DROP EXTENSION <ExtensionName> ";"


Description
-----------

``DROP EXTENSION`` disables a currently active extension for the
current database.


Examples
--------

Disable :ref:`GraphQL <ref_graphql_index>` extension for the current
schema:

.. code-block:: edgeql

    DROP EXTENSION graphql;

Disable :ref:`EdgeQL over HTTP <ref_edgeql_index>` extension for the
current database:

.. code-block:: edgeql

    DROP EXTENSION edgeql_http;


