.. _ref_datamodel_overview:

========
Overview
========

EdgeDB is an object-relational database with strongly typed schema.


.. _ref_datamodel_typesystem:

Type System
===========

An EdgeDB schema is primarily composed from *object type* definitions, which
describe entities in a specific domain.  An *object type* is a collection
of named *properties* and *links* to other types.

Here is an example of a simple EdgeDB type using the Edge Schema notation:

.. code-block:: eschema

    type User:
        property name -> str
        property address -> str
        link friends -> User

``str`` in the above example is a
:ref:`scalar type <ref_datamodel_scalar_types>`.  EdgeDB also supports
several :ref:`collection types <ref_datamodel_collection_types>`.  Scalar
types and collection types are collectively called *primitive types* in
contrast with object types.


Modules and Items
=================

EdgeDB schemas consist of :ref:`modules <ref_datamodel_modules>`.  Modules
contain *schema items*.

There are several kinds of schema items:

* :ref:`object type definitions <ref_datamodel_object_types>`
* :ref:`scalar type definitions <ref_datamodel_scalar_types>`
* :ref:`link definitions <ref_datamodel_links>`
* :ref:`property definitions <ref_datamodel_props>`
* :ref:`constraint definitions <ref_datamodel_constraints>`
* :ref:`schema attribute definitions <ref_datamodel_attributes>`
* :ref:`function definitions <ref_datamodel_functions>`

There are also a special types ``anytype`` and ``anytuple`` used to
define polymorphic parameters in functions and operators:

.. eql:type:: anytype

    :index: any anytype

    Generic type.

.. eql:type:: anytuple

    :index: any anytuple

    Generic :eql:type:`tuple`.


.. _ref_datamodel_inheritance:

Inheritance
===========

Most items in EdgeDB schema support *inheritance* as a composition mechanism.
Schema items can *extend* other item(s) of the same kind.  When extending,
*child* items inherit the aspects of the *parent* item(s) in a manner specific
to the schema item kind.  For example, when an object type extends another
object type, it inherits all parent properties, links, constraints and other
aspects.  Additionally, for instances of the child type
``object IS ParentType`` is ``True`` (see :eql:op:`IS operator <IS>`).  Also,
instances of the child type are included in the set of all instances of
the parent type.


EdgeDB Schema
=============

:ref:`EdgeDB Schema <ref_eschema>` is a high-level declarative alternative to
:ref:`EdgeQL data definition<ref_eql_ddl>` commands.  It is designed to
be a consise and readable representation of schema state.  Most of the examples
and synopses in this section use the EdgeDB Schema notation.
