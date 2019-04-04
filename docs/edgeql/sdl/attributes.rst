.. _ref_eql_sdl_schema_attributes:

==========
Attributes
==========

This section describes the SDL declarations pertaining to
:ref:`attributes <ref_datamodel_attributes>`.


Examples
--------

Declare a new attribute:

.. code-block:: sdl

    abstract attribute admin_note;

Specify the value of an attribute for a type:

.. code-block:: sdl

    type Status {
        attribute admin_note := 'system-critical';
        required property name -> str {
            constraint exclusive
        }
    }


Syntax
------

Define a new attribute corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_schema_attributes>`.

.. sdl:synopsis::

    [ abstract ] [ inheritable ] attribute <name>
    [ "{" <attribute-declarations>; [...] "}" ] ;


Description
-----------

The core of the declaration is identical to
:eql:stmt:`CREATE ABSTRACT ATTRIBUTE`, while the valid SDL
sub-declarations are listed below:

:sdl:synopsis:`<attribute-declarations>`
    Attributes can also have attributes. Set the *attribute* of the
    enclosing attribute to a specific value.
