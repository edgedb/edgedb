.. _ref_datamodel_indexes:

=======
Indexes
=======

An :ref:`object type <ref_datamodel_object_types>` or an
:ref:`abstract link <ref_datamodel_links>` may define *indexes*.
An index is a special expression declaration which indicates to the
database that, for a given set of objects or links, a particular expression
must be *indexed* to allow for faster evaluation of queries which use
that expression.

The simplest form of index is an index, which references one
or more properties directly:

.. code-block:: eschema

    type User {
        property name -> str;
        index name_idx on (__subject__.name);
    }

With the above, ``User`` lookups by the ``name`` property will be faster,
as the database will not have to scan an entire set of objects sequentially
to find the matching objects:

.. code-block:: edgeql

    SELECT User FILTER User.name = 'Alice';

Indexes may be defined using an arbitrary expression that references properties
of the host object type or link:

.. code-block:: eschema

    type User {
        property firstname -> str;
        property lastname -> str;
        index name_idx on (str_lower(__subject__.firstname + ' ' +
                                     __subject__.lastname));
    }

The index expression must not reference any variables other than
the properties of the host object type or link.  All functions used
in the expression must not be set-returning.

.. note::

    While being beneficial to the speed of queries, indexes increase
    the database size and make insertion and updates slower, and creating
    too many indexes may be detrimental.


Definition
==========

Indexes may be defined in EdgeDB Schema in the context of a ``type`` or
``abstract link`` declaration using the following two forms:

.. eschema:synopsis::

    { type <TypeName> | abstract link <link-name> }:
        index <index-name> := <index-expr>

    { type <TypeName> | abstract link <link-name> }:
        index <index-name>:
            expr := <index-expr>
            [ <attr-name> := <attr-value> ]
            [ ... ]

Parameters
----------

:eschema:synopsis:`<index-name>`
    The name of the index.  No module name can be specified, indexes are
    always created in the same module as the host type or link.  Index
    names must be unique within their host.

:eschema:synopsis:`<index-expr>`
    An expression based on one or more properties of the host schema item.

:eschema:synopsis:`[ <attr-name> := <attr-value> ]`
    An optional list of schema attribute values for the index. See
    :ref:`schema attributes <ref_datamodel_attributes>` for more information.


DDL
===

Indexes can also be defined using the :eql:stmt:`CREATE INDEX` EdgeQL command.
