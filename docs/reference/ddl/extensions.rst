.. _ref_eql_ddl_extensions:

==========
Extensions
==========

This section describes the DDL commands pertaining to
:ref:`extensions <ref_datamodel_extensions>`.


Create extension
================

:eql-statement:

Enable a particular extension for the current schema.

.. eql:synopsis::

    create extension <ExtensionName> ";"

There's a :ref:`corresponding SDL declaration <ref_eql_sdl_extensions>`
for enabling an extension, which is the recommended way of doing this.

Description
-----------

The command ``create extension`` enables the specified extension for
the current :versionreplace:`database;5.0:branch`.

Examples
--------

Enable :ref:`GraphQL <ref_graphql_index>` extension for the current
schema:

.. code-block:: edgeql

    create extension graphql;

Enable :ref:`EdgeQL over HTTP <ref_edgeql_http>` extension for the
current :versionreplace:`database;5.0:branch`:

.. code-block:: edgeql

    create extension edgeql_http;


drop extension
==============

:eql-statement:


Disable an extension.

.. eql:synopsis::

    drop extension <ExtensionName> ";"


Description
-----------

The command ``drop extension`` disables a currently active extension for the
current :versionreplace:`database;5.0:branch`.


Examples
--------

Disable :ref:`GraphQL <ref_graphql_index>` extension for the current
schema:

.. code-block:: edgeql

    drop extension graphql;

Disable :ref:`EdgeQL over HTTP <ref_edgeql_http>` extension for the
current :versionreplace:`database;5.0:branch`:

.. code-block:: edgeql

    drop extension edgeql_http;


