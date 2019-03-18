.. _ref_eschema:

=============
EdgeDB Schema
=============

This section describes the general lexical and syntactic structure of
the EdgeDB Schema language.  Definition syntax for specific schema items is
provided in the corresponding section of the
:ref:`Data Model <ref_datamodel_overview>` section.

EdgeDB Schema is a declarative language optimized for human readability.
It uses whitespace indentation to denote structure.  Each EdgeDB Schema file
represents a complete schema state for a particular
:ref:`module <ref_datamodel_modules>`.


.. _ref_eschema_migrations:

Migrations
==========

To apply a Schema to the database, the :eql:stmt:`CREATE MIGRATION`
command must be used followed by :eql:stmt:`COMMIT MIGRATION`:

.. code-block:: edgeql

    START TRANSACTION;

    CREATE MIGRATION init TO {
        type User {
            property name -> str
        }
    };

    COMMIT MIGRATION init;

    COMMIT;


.. toctree::
    :maxdepth: 3
    :hidden:

    lexical
