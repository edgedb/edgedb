.. _ref_datamodel_attributes:

=================
Schema Attributes
=================

*Schema attributes* are named values associated with schema items and are
designed to hold arbitrary schema-level metadata.

Every schema attribute is declared to have a specific
:ref:`scalar type <ref_datamodel_scalar_types>` or a
:ref:`collection type <ref_datamodel_collection_types>`.


Definition
==========

An attribute may be defined in EdgeDB Schema using the ``attribute``
declaration:

.. eschema:synopsis::

    attribute <attr-name> -> <type>:
        [ <attribute-declarations> ]

Parameters:

:eschema:synopsis:`<attr-name>`
    Specifies the name of the attribute.  Customarily, attribute names
    are lowercase, with words separated by underscores as necessary for
    readability.

:eschema:synopsis:`<type>`
    Specifies attribute data type.  Must be a valid
    :ref:`scalar type <ref_datamodel_scalar_types>` or a
    :ref:`collection type <ref_datamodel_collection_types>`.

:eschema:synopsis:`<attribute-declarations>`
    Schema attribute declarations for this attribute.  Schema attributes
    are considered schema items, and can have attributes themselves.


Attributes can also be defined using the :eql:stmt:`CREATE ATTRIBUTE`
EdgeQL command.


Setting Attributes
==================

Attributes may be set in EdgeDB Schema using the following syntax:

.. eschema:synopsis::

    <attr-name> := <constant-value>

Here :eschema:synopsis:`<attr-name>` is the name of the previously
defined attribute, and :eschema:synopsis:`<constant-value>`
is a valid constant literal which type matches that of the attribute.

For example:

.. code-block:: eschema

    scalar type pr_status extending str:
        attribute title := 'Pull Request Status Type'

Attributes can also be set using the :eql:stmt:`SET ATTRIBUTE` EdgeQL command.


Standard Attributes
===================

There is a number of attributes defined in the standard library.  The following
are the attributes which can be set on any schema item:

- ``title``
- ``description``
