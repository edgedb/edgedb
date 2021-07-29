.. _ref_datamodel_links:

=====
Links
=====

:index: link one-to-one one-to-many many-to-one many-to-many

Link items define a specific relationship between two :ref:`object
types <ref_datamodel_object_types>`.  Link instances relate one
*object* to one or more different objects.

There are two kinds of link item declarations: *abstract links*, and
*concrete links*.  Abstract links are defined on the module level and are
not tied to any particular object type. Typically this is done to set
some :ref:`annotations <ref_datamodel_annotations>`, define
:ref:`link properties <ref_datamodel_props>`, or setup
:ref:`constraints <ref_datamodel_constraints>`.  Concrete links
are defined on specific object types.

Links are directional and have a *source*. Whether a *source* has one
or more links of the same kind is specified by the keywords
:ref:`single <ref_eql_ddl_links_syntax>` and :ref:`multi
<ref_eql_ddl_links_syntax>`, respectively. The :ref:`required
<ref_eql_ddl_links_syntax>` keyword indicates that at least one
target object must be linked for a particular link kind. It is
also possible to restrict how many source objects can link to the
same target via the :eql:constraint:`exclusive` constraint. Using
these tools it's possible to specify common relationships between
things: *many-to-one*, *one-to-one*, and *many-to-many*.

It is possible to think of any link as going backwards from *target*
to *source*. This is referred to as a *backlink* and we use the ``.<``
:ref:`syntax <ref_eql_expr_paths>` to denote it.


Many-to-One
-----------

A *many-to-one* relationship is a fairly common pattern representing
situations like ownership or hierarchies. For example, ``Person`` and
``Shirt``:

.. code-block:: sdl

    type Person {
        required property name -> str {
            constraint exclusive;
        }
    }
    type Shirt {
        required property description -> str {
            # Just making sure that each description
            # is unique like a name.
            constraint exclusive;
        }
        link owner -> Person;
    }

A ``Shirt`` can have at most one ``owner``, while a ``Person`` can
have potentially have more than one ``Shirt``. This is a *many-to-one*
relationship and it's expressed by the ``link owner``.

Selecting the shirts belonging to a specific owner can be done with
the following query:

.. code-block:: edgeql

    SELECT Shirt {
        description,
        owner: {
            name
        }
    }
    FILTER .owner.name = 'Billie';

When a *many-to-one* link is treated as a *backlink* it becomes a
*one-to-many* relationship instead. For example, the previous query
can be re-written like this:

.. code-block:: edgeql

    SELECT Person {
        name,
        # let's use a computable here
        shirts := .<owner[IS Shirt] {
            description
        }
    }
    FILTER .name = 'Billie';

Alternatively, the above relationship can also be represented by the
following schema:

.. code-block:: sdl

    type Person {
        required property name -> str {
            constraint exclusive;
        }
        multi link shirts -> Shirt {
            # The exclusive constraint ensures that
            # this is a one-to-many relationship.
            constraint exclusive;
        }
    }
    type Shirt {
        required property description -> str {
            constraint exclusive;
        }
    }

It's possible to include both links ``owner`` and ``shirts`` to a
schema, making one of them a :ref:`computable link
<ref_datamodel_computables>` expressed in terms of the other.

.. code-block:: sdl

    type Person {
        required property name -> str {
            constraint exclusive;
        }
        # A computable link used for convenience.
        multi link shirts := .<owner[IS Shirt];
    }
    type Shirt {
        required property description -> str {
            # Just making sure that each description
            # is unique like a name.
            constraint exclusive;
        }
        link owner -> Person;
    }

So fundamentally there's no difference in terms of the data for the
two schemas specifying many-to-one or one-to-many relationship between
``Person`` and ``Shirt``. Nor is there any difference in terms of
querying that data, because computable links can be added to the
schema. Instead the difference is in how the data is modified or
reasoned about. For example, expressing "Billie bought some yellow
shirts" using the first and second version of the schema would look
like this:

.. code-block:: edgeql

    UPDATE Shirt
    # Just get all the yellow ones
    FILTER .description ILIKE '%yellow%'
    SET {
        owner := (
            SELECT Person
            FILTER .name = 'Billie'
        )
    };

    UPDATE Person
    FILTER .name = 'Billie'
    SET {
        shirts += (
            SELECT Shirt
            # Just get all the yellow ones
            FILTER .description ILIKE '%yellow%'
        )
    };


One-to-One
----------

A *one-to-one* relationship represents a situation where one object
from a source set is linked to only one object in the target set, and
vice versa. For example, ``Employee`` and ``ReservedParking``:

.. code-block:: sdl

    type Employee {
        required property name -> str;
        single link parking -> ReservedParking {
            constraint exclusive;
        }
    }
    type ReservedParking {
        required property number -> int64;
    }

An ``Employee`` can have up to one ``ReservedParking`` assigned
exclusively to them. The :eql:constraint:`exclusive` constraint
ensures that no more than *one* ``Employee`` can get the same
``ReservedParking``, while the ``single`` qualifier on the link (which
is the default, so it can be omitted) ensures that no ``Employee`` can
have more than *one* ``ReservedParking``. Together the constraint and
the qualifier specify a *one-to-one* relationship.

Although the link is specified only on one of the objects, the
relationship involves both of them and so it can be accessed from
either end. To get the assigned ``ReservedParking`` given an
``Employee`` the following query can be used:

.. code-block:: edgeql

    WITH Alice := (
        SELECT Employee FILTER .name = 'Alice'
    )
    SELECT Alice.parking {
        number
    };

The reverse lookup of who owns a particular ``ReservedParking`` spot
can be done by using a *backlink* traversal like so:

.. code-block:: edgeql

    WITH Spot := (
        SELECT ReservedParking FILTER .number = 42
    )
    SELECT Spot.<parking[IS Employee] {
        name
    };

In practice, *backlink* traversal requires to specify the original
link's source type, but other than that it works the same way as
forward traversal.


Many-to-Many
------------

A *many-to-many* relationship represents the most generic kind of
relationship without any exclusivity. For example, ``Person`` and
``Movie`` in the following schema:

.. code-block:: sdl

    type Person {
        required property name -> str {
            constraint exclusive;
        }
        multi link likes -> Movie;
    }
    type Movie {
        required property title -> str {
            constraint exclusive;
        }
    }

A ``Person`` can like multiple movies and each ``Movie`` can be liked
by multiple people, thus making ``likes`` a *many-to-many*
relationship. This type of relationship has the same symmetry as a
*one-to-one* w.r.t. regular link and *backlink* traversal, except that
potentially multiple objects can be reached in either direction.
Here's the query for getting every ``Movie`` a given ``Person`` likes:

.. code-block:: edgeql

    WITH Cameron := (
        SELECT Person FILTER .name = 'Cameron'
    )
    SELECT Cameron.likes {
        title
    };

The *backlink* lookup of who likes a particular ``Movie``:

.. code-block:: edgeql

    WITH M := (
        SELECT Movie FILTER .title = "Matrix"
    )
    SELECT M.<likes[IS Person] {
        name
    };


Deletion
--------

Links also have a policy of handling link target *deletion*. There are
4 possible *actions* that can be taken when this happens:

- ``RESTRICT`` - any attempt to delete the target object immediately
  raises an exception;
- ``DELETE SOURCE`` - when the target of a link is deleted, the source
  is also deleted;
- ``ALLOW`` - the target object is deleted and is removed from the
  set of the link targets;
- ``DEFERRED RESTRICT`` - any attempt to delete the target object
  raises an exception at the end of the transaction, unless by
  that time this object is no longer in the set of link targets.

This :ref:`section <ref_eql_ddl_links_syntax>` covers the syntax of
how to set these policies in more detail.

See Also
--------

:ref:`Cookbook <ref_cookbook_links>` section about links.

Link
:ref:`SDL <ref_eql_sdl_links>`,
:ref:`DDL <ref_eql_ddl_links>`,
:ref:`introspection <ref_eql_introspection_object_types>`
(as part of overall object introspection).
