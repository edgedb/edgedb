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

.. code-block:: sdl

    type User {
        property name -> str;
        index name_idx on __subject__.name;
    }

With the above, ``User`` lookups by the ``name`` property will be faster,
as the database will not have to scan an entire set of objects sequentially
to find the matching objects:

.. code-block:: edgeql

    SELECT User FILTER User.name = 'Alice';

Indexes may be defined using an arbitrary expression that references properties
of the host object type or link:

.. code-block:: sdl

    type User {
        property firstname -> str;
        property lastname -> str;
        index name_idx on str_lower(
            __subject__.firstname + ' ' + __subject__.lastname);
    }

The index expression must not reference any variables other than
the properties of the host object type or link.  All functions used
in the expression must not be set-returning.

.. note::

    While being beneficial to the speed of queries, indexes increase
    the database size and make insertion and updates slower, and creating
    too many indexes may be detrimental.
