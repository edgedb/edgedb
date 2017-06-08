.. _ref_edgeql_shapes:


Shapes
======

Shapes are a way of specifying which data should be retrieved for each
object. This annotation does not actually alter the objects in any
way, but rather provides a guideline for serialization.

Shapes define the *relationships structure* of the data that is
retrieved from the DB. Thus shapes themselves are a lexical
specification used with valid expressions denoting objects. There's no
need to explicitly include ``id`` in the shape specification because
it is always implicitly included since the shape is always based on an
object.

Shapes allow retrieving not only a set of objects, but to also
represent that set as a *forest*, where each base object is the root
of a *tree*. Technically, this set of trees is a directed graph
possibly even containing cycles. However, the serialized
representation is based on a set of trees (or nested JSON).

Another use of shapes is *augmentation* of the object data. This can
be useful for serialization, but also as a convenient way of computing
some values used for filtering.

For example it's possible to augment each user object with the
information about how many issues they have:

.. code-block:: eql

    SELECT User {
        name,
        issues := count(User.<owner[IS Issue])
    };

Similarly, we can add a filter based on the number of issues that a
user has by referring to the :ref:`computable<ref_edgeql_computables>`
defined by the shape:

.. code-block:: eql

    SELECT User {
        name,
        issues := count(User.<owner[IS Issue])
    } FILTER User.issues > 5;

In order to refer to :ref:`computables<ref_edgeql_computables>` a
shape must be in the same lexical statement as the expression
referring to it.

.. note::

    Shapes serve an important function of pre-fetching specific data
    and *that data only* when serialized. For example, it's possible
    to fetch all issues with ``watchers`` restricted to a specific
    subset of users, then in the processing code safely refer to
    ``issue.watchers`` without further restrictions and only access
    the restricted set of watchers that was fetched.

    .. code-block:: eql

        SELECT Issue {
            name,
            text,
            # we only want real watchers, not internal
            # system accounts
            watchers: {
                name
            } FILTER Issue.watchers IS NOT SystemUser
        };
