.. _ref_eql_ddl_indexes:

=======
Indexes
=======

This section describes the DDL commands pertaining to
:ref:`indexes <ref_datamodel_indexes>`.


CREATE INDEX
============

:eql-statement:


Define an new :ref:`index <ref_datamodel_indexes>` for a given object
type or link.

.. eql:synopsis::

    CREATE INDEX <index-name> := <index-expr> ;

    CREATE INDEX <index-name> "{"
        SET expr := <index-expr>;
        [ <action >; ... ]
    "}" ;


Description
-----------

``CREATE INDEX`` constructs a new index *index-name* for a given object
type or link using *index-expr*.


Parameters
----------

:eql:synopsis:`<index-name>`
    The name of the index to be created.  No module name can be specified,
    indexes are always created in the same module as the parent type or
    link.


Examples
--------

Create an object type ``User`` with an indexed ``title`` property:

.. code-block:: edgeql

    CREATE TYPE User {
        CREATE PROPERTY title -> std::str {
            SET default := '';
        };

        CREATE INDEX title_name ON __subject__.title;
    };


DROP INDEX
==========

:eql-statement:

Remove an index from a given schema item.

.. eql:synopsis::

    DROP INDEX <index-name> ;

Description
-----------

``DROP INDEX`` removes an index from a schema item.

:eql:synopsis:`<index-name>`
    Refers to the name of a defined index.

This statement can only be used as a subdefinition in another
DDL statement.


Examples
--------

Drop the ``title`` index from the ``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        DROP INDEX title;
    };
