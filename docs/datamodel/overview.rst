.. _ref_datamodel_overview:

========
Overview
========

EdgeDB is a relational database with strongly typed schema.


.. _ref_datamodel_typesystem:

Type System
===========

An EdgeDB schema is primarily composed from *object type* definitions, which
describe entities in a specific domain.  An *object type* is a collection
of named *properties* and *links* to other types.

Here is an example of a simple EdgeDB type using the Edge Schema notation:

.. code-block:: sdl

    type User {
        property name -> str;
        property address -> str;
        multi link friends -> User;
    }

``str`` in the above example is a
:ref:`scalar type <ref_datamodel_scalar_types>`.  EdgeDB also supports
:ref:`collection types <ref_datamodel_collection_types>`.  Scalar
types and collection types are collectively called *primitive types* in
contrast with object types.


Modules and Items
=================

EdgeDB schemas consist of :ref:`modules <ref_datamodel_modules>`.  Modules
contain *schema items*.

There are several kinds of schema items:

* :ref:`object types <ref_datamodel_object_types>`
* :ref:`scalar types <ref_datamodel_scalar_types>`
* :ref:`links <ref_datamodel_links>`
* :ref:`properties <ref_datamodel_props>`
* :ref:`constraints <ref_datamodel_constraints>`
* :ref:`annotations <ref_datamodel_annotations>`
* :ref:`functions <ref_datamodel_functions>`


.. _ref_datamodel_inheritance:

Inheritance
===========

Most items in EdgeDB schema support *inheritance* as a composition mechanism.
Schema items can *extend* other item(s) of the same kind.  When extending,
*child* items inherit the aspects of the *parent* item(s) in a manner specific
to the schema item kind.  For example, when an object type extends another
object type, it inherits all parent properties, links, constraints and other
aspects.  Additionally, for instances of the child type
``object IS ParentType`` is ``true`` (see :eql:op:`IS operator <IS>`).  Also,
instances of the child type are included in the set of all instances of
the parent type.

Multiple inheritance allows composing several types into one. A common
pattern is to have many basic abstract types (such as ``Named``,
``HasEmail``, ``HasAddress``, etc.) each with their own links and
properties and then extending different combinations of them.

Finally, various inheritance structures enable the use of
:ref:`polymorphic queries <ref_eql_polymorphic_queries>`.


EdgeDB Schema
=============

:ref:`EdgeDB schema definition language<ref_eql_sdl>` is a high-level
declarative alternative to :ref:`EdgeDB data definition<ref_eql_ddl>`
commands.  It is designed to be a concise and readable representation
of schema state.  Most of the examples and synopses in this section
use the SDL notation.
