.. _ref_eql_ddl_schema_attributes:

=================
Schema Attributes
=================

This section describes the DDL commands pertaining to
:ref:`schema attributes <ref_datamodel_attributes>`.


CREATE ABSTRACT ATTRIBUTE
=========================

:eql-statement:

Define a new :ref:`schema attribute <ref_datamodel_attributes>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE ABSTRACT [ INHERITABLE ] ATTRIBUTE <name>
    [ "{" <subdefinition>; [...] "}" ] ;


Description
-----------

``CREATE ABSTRACT ATTRIBUTE`` defines a new schema attribute for use in the
current database.

If *name* is qualified with a module name, then the attribute is created
in that module, otherwise it is created in the current module.
The attribute name must be distinct from that of any existing schema item
in the module.

The attributes are non-inheritable by default.  That is, if a schema item
has an attribute defined on it, the descendants of that schema item will
not automatically inherit the attribute.  Normal inheritance behavior can
be turned on by declaring the attribute with the *INHERITABLE* qualifier.

:eql:synopsis:`<subdefinition>`
    Optional sequence of subdefinitions related to the new attribute.

    The following subdefinitions are allowed in the
    ``CREATE ABSTRACT ATTRIBUTE`` block:

    * :eql:stmt:`SET ATTRIBUTE`


Examples
--------

Set the attribute ``title`` of object type ``User`` to ``"User"``:

.. code-block:: edgeql

    ALTER TYPE User SET ATTRIBUTE title := "User";


DROP ABSTRACT ATTRIBUTE
=======================

:eql-statement:

Remove a :ref:`schema attribute <ref_datamodel_attributes>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP ABSTRACT ATTRIBUTE <name> ;

Description
-----------

``DROP ABSTRACT ATTRIBUTE`` removes an existing schema attribute from
the database schema.  Note that the ``INHERITABLE`` qualifier is not
necessary in this statement.

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

    SET ATTRIBUTE <attribute> := <value>

Description
-----------

``SET ATTRIBUTE`` defines an attribute value for a schema item.

*attribute* refers to the name of a defined attribute, and
*value* must be a constant EdgeQL expression evaluating into a string.

This statement can only be used as a subdefinition in another
DDL statement.


Examples
--------

Create an object type ``User`` and set its ``title`` attribute to
``"User type"``.

.. code-block:: edgeql

    CREATE TYPE User {
        SET ATTRIBUTE title := 'User type';
    };



DROP ATTRIBUTE
==============

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
