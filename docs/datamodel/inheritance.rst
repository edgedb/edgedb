.. _ref_datamodel_inheritance:

===========
Inheritance
===========

Inheritance is a crucial aspect of schema modeling in EdgeDB.

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
