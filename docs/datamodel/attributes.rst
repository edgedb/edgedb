.. _ref_datamodel_attributes:

=================
Schema Attributes
=================

*Schema attributes* are named values associated with schema items and
are designed to hold arbitrary schema-level metadata represented as a
:eql:type:`str`.


Definition
==========

An attribute may be defined in EdgeDB Schema using the ``attribute``
declaration:

.. eschema:synopsis::

    abstract [ inheritable ] attribute <attr-name>:
        [ <attribute-declarations> ]

Parameters:

:eschema:synopsis:`<attr-name>`
    Specifies the name of the attribute.  Customarily, attribute names
    are lowercase, with words separated by underscores as necessary for
    readability.

:eschema:synopsis:`inheritable`
    The attributes are non-inheritable by default.  That is, if a schema item
    has an attribute defined on it, the descendants of that schema item will
    not automatically inherit the attribute.  Normal inheritance behavior can
    be turned on by declaring the attribute with the *inheritable* qualifier.

:eschema:synopsis:`<attribute-declarations>`
    Schema attribute declarations for this attribute.  Schema attributes
    are considered schema items, and can have attributes themselves.


Attributes can also be defined using the :eql:stmt:`CREATE ABSTRACT ATTRIBUTE`
EdgeQL command.


Setting Attributes
==================

Attributes may be set in EdgeDB Schema using the following syntax:

.. eschema:synopsis::

    attribute <attr-name> := <constant-string-value>

Here :eschema:synopsis:`<attr-name>` is the name of the previously
defined attribute, and :eschema:synopsis:`<constant-string-value>`
is a valid constant expression that evaluates to a string.

For example:

.. code-block:: eschema

    scalar type pr_status extending str {
        attribute title := 'Pull Request Status Type';
    }

Attributes can also be set using the :eql:stmt:`SET ATTRIBUTE` EdgeQL command.


Standard Attributes
===================

There is a number of attributes defined in the standard library.  The following
are the attributes which can be set on any schema item:

- ``title``
- ``description``
