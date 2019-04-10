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

    type Status {
        annotation admin_note := 'system-critical';
        required property name -> str {
            constraint exclusive
        }
    }


Syntax
------

Define a new annotation corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_annotations>`.

.. sdl:synopsis::

    [ abstract ] [ inheritable ] annotation <name>
    [ "{" <annotation-declarations>; [...] "}" ] ;


Description
-----------

The core of the declaration is identical to
:eql:stmt:`CREATE ABSTRACT ANNOTATION`, while the valid SDL
sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Annotations can also have annotations. Set the *annotation* of the
    enclosing annotation to a specific value.
