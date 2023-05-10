.. _ref_eql_sdl_annotations:

===========
Annotations
===========

This section describes the SDL declarations pertaining to
:ref:`annotations <ref_datamodel_annotations>`.


Examples
--------

Declare a new annotation:

.. code-block:: sdl

    abstract annotation admin_note;

Specify the value of an annotation for a type:

.. code-block:: sdl
    :version-lt: 3.0

    type Status {
        annotation admin_note := 'system-critical';
        required property name -> str {
            constraint exclusive
        }
    }

.. code-block:: sdl

    type Status {
        annotation admin_note := 'system-critical';
        required name: str {
            constraint exclusive
        }
    }

.. _ref_eql_sdl_annotations_syntax:

Syntax
------

Define a new annotation corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_annotations>`.

.. sdl:synopsis::

    # Abstract annotation form:
    abstract [ inheritable ] annotation <name>
    [ "{" <annotation-declarations>; [...] "}" ] ;

    # Concrete annotation (same as <annotation-declarations>) form:
    annotation <name> := <value> ;


Description
-----------

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

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Annotations <ref_datamodel_annotations>`
  * - :ref:`DDL > Annotations <ref_eql_ddl_annotations>`
  * - :ref:`Cheatsheets > Annotations <ref_cheatsheet_annotations>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
