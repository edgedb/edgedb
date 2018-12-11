.. _ref_eql_ddl_schema_attributes:

=================
Schema Attributes
=================

This section describes the DDL commands pertaining to
:ref:`schema attributes <ref_datamodel_attributes>`.


CREATE ATTRIBUTE
================

:eql-statement:

Define a new :ref:`schema attribute <ref_datamodel_attributes>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE ATTRIBUTE <name> <typename>
    [ "{" <subdefinition>; [...] "}" ] ;


Description
-----------

``CREATE ATTRIBUTE`` defines a new schema attribute for use in the
current database.

If *name* is qualified with a module name, then the attribute is created
in that module, otherwise it is created in the current module.
The attribute name must be distinct from that of any existing schema item
in the module.

*typename* is a possibly fully-qualified name specifying the data type
of the new attribute; it must refer to a primitive type.

:eql:synopsis:`<subdefinition>`
    Optional sequence of subdefinitions related to the new attribute.

    The following subdefinitions are allowed in the ``CREATE ATTRIBUTE``
    block:

    * :eql:stmt:`SET <SET ATTRIBUTE>`


Examples
--------

Set the attribute ``title`` of object type ``User`` to ``"User"``:

.. code-block:: edgeql

    ALTER TYPE User SET title := "User";


DROP ATTRIBUTE
==============

:eql-statement:

Remove a :ref:`schema attribute <ref_datamodel_attributes>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP ATTRIBUTE <name> ;

Description
-----------

``DROP ATTRIBUTE`` removes an existing schema attribute from the database
schema.

Examples
--------

Drop the attribute ``extrainfo``:

.. code-block:: edgeql

    DROP ABSTRACT ATTRIBUTE extrainfo;


SET ATTRIBUTE
=============

:eql-statement:

Define an attribute value for a given schema item.

.. eql:synopsis::

    SET <attribute> := <value>

Description
-----------

``SET`` defines an attribute value for a schema item.

*attribute* refers to the name of a defined attribute, and
*value* must be a constant EdgeQL expression of the type matching
the attribute data type declaration.

This statement can only be used as a subdefinition in another
DDL statement.


Examples
--------

Create an object type ``User`` and set its ``title`` attribute to
``"User type"``.

.. code-block:: edgeql

    CREATE TYPE User {
        SET title := 'User type';
    };



DROP ATTRIBUTE VALUE
====================

:eql-statement:


Remove an attribute value from a given schema item.

.. eql:synopsis::

    DROP ATTRIBUTE <attribute> ;

Description
-----------

``DROP ATTRIBUTE`` removes an attribute value from a schema item.

*attribute* refers to the name of a defined attribute.  The attribute
value does not have to exist on a schema item.

This statement can only be used as a subdefinition in another
DDL statement.


Examples
--------

Drop the ``title`` attribute from the ``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        DROP ATTRIBUTE title;
    };
