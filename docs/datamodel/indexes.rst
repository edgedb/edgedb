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

The *subject* of an index is either the object or the abstract link on
which the index is defined. It can be referred to in the index
expression as ``__subject__``.

The simplest form of index is an index, which references one
or more properties directly:

.. code-block:: sdl

    type User {
        property name -> str;
        index on (__subject__.name);
    }

With the above, ``User`` lookups by the ``name`` property will be faster,
as the database will not have to scan an entire set of objects sequentially
to find the matching objects:

.. code-block:: edgeql

    SELECT User FILTER User.name = 'Alice';

Indexes may be defined using an arbitrary expression that references
properties of the host object type:

.. code-block:: sdl

    type User {
        property firstname -> str;
        property lastname -> str;
        index on (str_lower(
            __subject__.firstname + ' ' + __subject__.lastname));
    }

Similarly indexes may refer to the link properties if the *subject* is a link:

.. code-block:: sdl

    abstract link friends_base {
        property nickname -> str;
        index on (__subject__@nickname);
    }

The index expression must not reference any variables other than the
properties of the index *subject*.  All functions used in the
expression must not be set-returning.

Typically the explicit ``__subject__`` can be omitted in favor of a
short-form expression, since the index always appears nested inside
its *subject*:

.. code-block:: sdl

    type User {
        property name -> str;
        index on (.name);
    }

There's no need to create an index on just the link itself, as indexes
are already created for links implicitly. Also, as a special case,
adding the :eql:constraint:`exclusive` constraint to a property
implicitly creates an index for it as well.

.. note::

    While being beneficial to the speed of queries, indexes increase
    the database size and make insertion and updates slower, and creating
    too many indexes may be detrimental.



See Also
--------

Index
:ref:`SDL <ref_eql_sdl_indexes>`,
:ref:`DDL <ref_eql_ddl_indexes>`,
and :ref:`introspection <ref_eql_introspection_indexes>`.
