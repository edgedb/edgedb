.. _ref_eql_ddl_annotations:

===========
Annotations
===========

This section describes the DDL commands pertaining to
:ref:`annotations <ref_datamodel_annotations>`.


CREATE ABSTRACT ANNOTATION
==========================

:eql-statement:

:ref:`Define <ref_eql_sdl_annotations>` a new annotation.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    CREATE ABSTRACT [ INHERITABLE ] ANNOTATION <name>
    [ "{"
        CREATE ANNOTATION <annotation-name> := <value> ;
        [...]
      "}" ] ;


Description
-----------

``CREATE ABSTRACT ANNOTATION`` defines a new annotation for use in the
current database.

If *name* is qualified with a module name, then the annotation is created
in that module, otherwise it is created in the current module.
The annotation name must be distinct from that of any existing schema item
in the module.

The annotations are non-inheritable by default.  That is, if a schema item
has an annotation defined on it, the descendants of that schema item will
not automatically inherit the annotation.  Normal inheritance behavior can
be turned on by declaring the annotation with the *INHERITABLE* qualifier.

The following subcommands are allowed in the
``CREATE ABSTRACT ANNOTATION`` block:

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>`
    Annotations can also have annotations. Set the
    :eql:synopsis:`<annotation-name>` of the
    enclosing annotation to a specific :eql:synopsis:`<value>`.
    See :eql:stmt:`CREATE ANNOTATION` for details.


Example
-------

Declare an annotation ``extrainfo``.

.. code-block:: edgeql

    CREATE ABSTRACT ANNOTATION extrainfo;


DROP ABSTRACT ANNOTATION
========================

:eql-statement:

Remove a :ref:`schema annotation <ref_datamodel_annotations>`.

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]
    DROP ABSTRACT ANNOTATION <name> ;

Description
-----------

``DROP ABSTRACT ANNOTATION`` removes an existing schema annotation from
the database schema.  Note that the ``INHERITABLE`` qualifier is not
necessary in this statement.

Example
-------

Drop the annotation ``extrainfo``:

.. code-block:: edgeql

    DROP ABSTRACT ANNOTATION extrainfo;


CREATE ANNOTATION
=================

:eql-statement:

Define an annotation value for a given schema item.

.. eql:synopsis::

    CREATE ANNOTATION <annotation-name> := <value>

Description
-----------

``CREATE ANNOTATION`` defines an annotation for a schema item.

:eql:synopsis:`<annotation-name>` refers to the name of a defined annotation,
and :eql:synopsis:`<value>` must be a constant EdgeQL expression
evaluating into a string.

This statement can only be used as a subcommand in another
DDL statement.


Example
-------

Create an object type ``User`` and set its ``title`` annotation to
``"User type"``.

.. code-block:: edgeql

    CREATE TYPE User {
        CREATE ANNOTATION title := "User type";
    };


ALTER ANNOTATION
================

:eql-statement:

Alter an annotation value for a given schema item.

.. eql:synopsis::

    ALTER ANNOTATION <annotation-name> := <value>

Description
-----------

``ALTER ANNOTATION`` alters an annotation value on a schema item.

:eql:synopsis:`<annotation-name>` refers to the name of a defined annotation,
and :eql:synopsis:`<value>` must be a constant EdgeQL expression
evaluating into a string.

This statement can only be used as a subcommand in another
DDL statement.


Example
-------

Alter an object type ``User`` and alter the value of its previously set
``title`` annotation to ``"User type"``.

.. code-block:: edgeql

    ALTER TYPE User {
        ALTER ANNOTATION title := "User type";
    };


DROP ANNOTATION
===============

:eql-statement:


Remove an annotation from a given schema item.

.. eql:synopsis::

    DROP ANNOTATION <annotation-name> ;

Description
-----------

``DROP ANNOTATION`` removes an annotation value from a schema item.

:eql:synopsis:`<annotaion_name>` refers to the name of a defined annotation.
The annotation value does not have to exist on a schema item.

This statement can only be used as a subcommand in another
DDL statement.


Example
-------

Drop the ``title`` annotation from the ``User`` object type:

.. code-block:: edgeql

    ALTER TYPE User {
        DROP ANNOTATION title;
    };
