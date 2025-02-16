.. _ref_datamodel_annotations:
.. _ref_eql_sdl_annotations:

===========
Annotations
===========

.. index:: annotation

*Annotations* are named values associated with schema items and are
designed to hold arbitrary schema-level metadata represented as a
:eql:type:`str` (unstructured text).

Users can store JSON-encoded data in annotations if they need to store
more complex metadata.


Standard annotations
====================

.. index:: title, description, deprecated

There are a number of annotations defined in the standard library. The
following are the annotations which can be set on any schema item:

- ``std::title``
- ``std::description``
- ``std::deprecated``

For example, consider the following declaration:

.. code-block:: sdl

  type Status {
    annotation title := 'Activity status';
    annotation description := 'All possible user activities';

    required name: str {
      constraint exclusive
    }
  }

And the ``std::deprecated`` annotation can be used to mark deprecated items
(e.g., :eql:func:`str_rpad`) and to provide some information such as what
should be used instead.

User-defined annotations
========================

.. index:: abstract annotation

To declare a custom annotation type beyond the three built-ins, add an abstract
annotation type to your schema. A custom annotation could be used to attach
arbitrary JSON-encoded data to your schema—potentially useful for introspection
and code generation.

.. code-block:: sdl

  abstract annotation admin_note;

  type Status {
    annotation admin_note := 'system-critical';
  }


.. _ref_eql_sdl_annotations_syntax:

Declaring annotations
=====================

This section describes the syntax to use annotations in your schema.

Syntax
------

.. sdl:synopsis::

  # Abstract annotation form:
  abstract [ inheritable ] annotation <name>
  [ "{" <annotation-declarations>; [...] "}" ] ;

  # Concrete annotation (same as <annotation-declarations>) form:
  annotation <name> := <value> ;

Description
^^^^^^^^^^^

There are two forms of annotation declarations: abstract and concrete.
The *abstract annotation* form is used for declaring new kinds of
annotation in a module. The *concrete annotation* declarations are
used as sub-declarations for all other declarations in order to
actually annotate them.

The annotation declaration options are as follows:

:eql:synopsis:`abstract`
  If specified, the annotation will be *abstract*.

:eql:synopsis:`inheritable`
  If specified, the annotation will be *inheritable*. The
  annotations are non-inheritable by default. That is, if a schema
  item has an annotation defined on it, the descendants of that
  schema item will not automatically inherit the annotation. Normal
  inheritance behavior can be turned on by declaring the annotation
  with the ``inheritable`` qualifier. This is only valid for *abstract
  annotation*.

:eql:synopsis:`<name>`
  The name (optionally module-qualified) of the annotation.

:eql:synopsis:`<value>`
  Any string value that the specified annotation is intended to have
  for the given context.

The only valid SDL sub-declarations are *concrete annotations*:

:sdl:synopsis:`<annotation-declarations>`
  Annotations can also have annotations. Set the *annotation* of the
  enclosing annotation to a specific value.


.. _ref_eql_ddl_annotations:

DDL commands
============

This section describes the low-level DDL commands for creating, altering,
and dropping annotations and abstract annotations.

Create abstract annotation
--------------------------

:eql-statement:

Define a new annotation.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  create abstract [ inheritable ] annotation <name>
  [
    "{"
      create annotation <annotation-name> := <value> ;
      [...]
    "}"
  ] ;

Description
^^^^^^^^^^^

The command ``create abstract annotation`` defines a new annotation
for use in the current Gel database.

If *name* is qualified with a module name, then the annotation is created
in that module, otherwise it is created in the current module.
The annotation name must be distinct from that of any existing schema item
in the module.

The annotations are non-inheritable by default. That is, if a schema item
has an annotation defined on it, the descendants of that schema item will
not automatically inherit the annotation. Normal inheritance behavior can
be turned on by declaring the annotation with the ``inheritable`` qualifier.

Most sub-commands and options of this command are identical to the
:ref:`SDL annotation declaration <ref_eql_sdl_annotations_syntax>`.
There's only one subcommand that is allowed in the ``create
annotation`` block:

:eql:synopsis:`create annotation <annotation-name> := <value>`
  Annotations can also have annotations. Set the
  :eql:synopsis:`<annotation-name>` of the
  enclosing annotation to a specific :eql:synopsis:`<value>`.
  See :eql:stmt:`create annotation` for details.

Example
^^^^^^^

Declare an annotation ``extrainfo``:

.. code-block:: edgeql

  create abstract annotation extrainfo;


Alter abstract annotation
-------------------------

:eql-statement:

Change the definition of an annotation.

.. eql:synopsis::

  alter abstract annotation <name>
  [ "{" ] <subcommand>; [...] [ "}" ];

  # where <subcommand> is one of

    rename to <newname>
    create annotation <annotation-name> := <value>
    alter annotation <annotation-name> := <value>
    drop annotation <annotation-name>

Description
^^^^^^^^^^^

:eql:synopsis:`alter abstract annotation` changes the definition of an
abstract annotation.

Parameters
^^^^^^^^^^

:eql:synopsis:`<name>`
  The name (optionally module-qualified) of the annotation to alter.

The following subcommands are allowed in the ``alter abstract annotation``
block:

:eql:synopsis:`rename to <newname>`
  Change the name of the annotation to :eql:synopsis:`<newname>`.

:eql:synopsis:`alter annotation <annotation-name> := <value>`
  Annotations can also have annotations. Change
  :eql:synopsis:`<annotation-name>` to a specific
  :eql:synopsis:`<value>`. See :eql:stmt:`alter annotation` for
  details.

:eql:synopsis:`drop annotation <annotation-name>`
  Annotations can also have annotations. Remove annotation
  :eql:synopsis:`<annotation-name>`.
  See :eql:stmt:`drop annotation` for details.

All the subcommands allowed in the ``create abstract annotation``
block are also valid subcommands for ``alter annotation`` block.

Example
^^^^^^^

Rename an annotation:

.. code-block:: edgeql

  alter abstract annotation extrainfo
    rename to extra_info;


Drop abstract annotation
------------------------

:eql-statement:

Drop a schema annotation.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  drop abstract annotation <name> ;

Description
^^^^^^^^^^^

The command ``drop abstract annotation`` removes an existing schema
annotation from the database schema. Note that the ``inheritable``
qualifier is not necessary in this statement.

Example
^^^^^^^

Drop the annotation ``extra_info``:

.. code-block:: edgeql

  drop abstract annotation extra_info;


Create annotation
-----------------

:eql-statement:

Define an annotation value for a given schema item.

.. eql:synopsis::

  create annotation <annotation-name> := <value>

Description
^^^^^^^^^^^

The command ``create annotation`` defines an annotation for a schema item.

:eql:synopsis:`<annotation-name>` refers to the name of a defined annotation,
and :eql:synopsis:`<value>` must be a constant EdgeQL expression
evaluating into a string.

This statement can only be used as a subcommand in another
DDL statement.

Example
^^^^^^^

Create an object type ``User`` and set its ``title`` annotation to
``"User type"``.

.. code-block:: edgeql

  create type User {
    create annotation title := "User type";
  };


Alter annotation
----------------

:eql-statement:

Alter an annotation value for a given schema item.

.. eql:synopsis::

  alter annotation <annotation-name> := <value>

Description
^^^^^^^^^^^

The command ``alter annotation`` alters an annotation value on a schema item.

:eql:synopsis:`<annotation-name>` refers to the name of a defined annotation,
and :eql:synopsis:`<value>` must be a constant EdgeQL expression
evaluating into a string.

This statement can only be used as a subcommand in another
DDL statement.

Example
^^^^^^^

Alter an object type ``User`` and alter the value of its previously set
``title`` annotation to ``"User type"``.

.. code-block:: edgeql

  alter type User {
    alter annotation title := "User type";
  };


Drop annotation
---------------

:eql-statement:

Remove an annotation from a given schema item.

.. eql:synopsis::

  drop annotation <annotation-name> ;

Description
^^^^^^^^^^^

The command ``drop annotation`` removes an annotation value from a schema item.

:eql:synopsis:`<annotaion_name>` refers to the name of a defined annotation.
The annotation value does not have to exist on a schema item.

This statement can only be used as a subcommand in another
DDL statement.

Example
^^^^^^^

Drop the ``title`` annotation from the ``User`` object type:

.. code-block:: edgeql

  alter type User {
    drop annotation title;
  };


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Cheatsheets > Annotations <ref_cheatsheet_annotations>`
  * - :ref:`Introspection > Object types <ref_datamodel_introspection_object_types>`
