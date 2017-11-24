.. _ref_edgeql_fundamentals:


Fundamentals
============

EdgeQL is the query language used to work with EdgeDB and it has been
designed to work with objects, their properties and relations. For the
purposes of this section we will use the following schema that is
assumed to be part of the module ``example``:

.. code-block:: eschema

    abstract concept Text:
        # This is an abstract object containing text.
        required link body to str:
            # Maximum length of text is 10000
            # characters.
            constraint maxlength(10000)

    concept User extending std::Named
    # NamedObject is a standard abstract base class,
    # that provides a name link.

    concept SystemUser extending std::User
    # a type of user that represents various automatic systems, that
    # might add comments to issues, perhaps based on some automatic
    # escalation system for unresolved higher priority issues

    abstract concept Owned:
        # By default links are optional.
        required link owner to User

    concept Status extending std::Dictionary
    # Dictionary is a NamedObject variant, that enforces
    # name uniqueness across all instances if its subclass.

    concept Priority extending std::Dictionary

    concept LogEntry extending OwnedObject, Text:
        # LogEntry is an OwnedObject and a Text, so it
        # will have all of their links and attributes,
        # in particular, owner and text links.
        required link spent_time to int

    atom issue_num_t extending std::sequence
    # issue_num_t is defined as a concrete sequence type,
    # used to generate sequential issue numbers.

    concept Comment extending Text, Owned:
        required link issue to Issue
        link parent to Comment

    concept Issue extending std::Named, Owned, Text:

        required link number to issue_num_t:
            readonly := true
            # The number values are automatically generated,
            # and are not supposed to be directly writable.

        required link status to Status

        link priority to Priority

        link watchers to User:
            mapping := '**'
            # The watchers link is mapped to User concept in
            # many-to-many relation.  The default mapping is
            # *1 -- many-to-one.

        link time_estimate to int

        link time_spent_log to LogEntry:
            mapping := '1*'
            # 1* -- one-to-many mapping.

        link start_date to datetime:
            default := SELECT datetime::current_datetime()
            # The default value of start_date will be a
            # result of the EdgeQL expression above.

        link due_date to datetime

        link related_to to Issue:
            mapping := '**'

This schema represents the data model for an issue tracker. There
are ``Users``, who can create an ``Issue``, add a ``Comment`` to an
``Issue``, or add a ``LogEntry`` to document work on a particular
``Issue``. ``Issues`` can be related to each other. A ``User`` can
watch any ``Issue``. Every ``Issue`` has a ``Status`` and possibly a
``Priority``.

The general structure of a simple EdgeQL query::

    [WITH [alias AS] MODULE module [,...] ]
    SELECT expression
    [FILTER expression]
    [ORDER BY expression [THEN ...]]
    [OFFSET expression]
    [LIMIT expression] ;

``SELECT``, ``FILTER``, ``ORDER BY``, ``OFFSET`` and ``LIMIT`` clauses
are explained in more details in the
:ref:`Statements<ref_edgeql_statements>` section. ``WITH`` is a
convenience clause that optionally :ref:`assigns aliases<ref_edgeql_with>`
being used in the query. In particular the most common use of the
``WITH`` block is to provide a default module for the query.

Note that the only required clause in the query is ``SELECT`` itself.
Expressions in all query clauses act as set generators. ``FILTER``
clause can be used to restrict the selected set and ``ORDER BY`` is
used for sorting. ``OFFSET`` and ``LIMIT`` are used to return only a
part of the selected set.

For example, a query to get all issues reported by Alice Smith:

.. code-block:: eql

    SELECT example::Issue
    FILTER example::Issue.owner.name = 'Alice Smith';

A somewhat neater way of writing the same query is:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue
    FILTER Issue.owner.name = 'Alice Smith';


Using expressions
-----------------

One of the basic units in EdgeQL are
:ref:`expressions<ref_edgeql_expressions>`. These always denote
objects or values. Basically, a concept instance is an object and
everything else is a value (more details can be found in the
:ref:`type system<ref_edgeql_types>` section).

.. code-block:: eql

    WITH MODULE example
    SELECT Issue
    FILTER Issue.owner.name = 'Alice Smith';

The above query has two examples of two kinds of expressions: path
expression and arithmetic expression.

Path expressions specify a set by starting with a concept and
following zero or more links from this concept to either atoms or
other concepts. The expressions ``Issue`` and ``Issue.owner.name`` are
examples of path expressions that point to a set of concepts and a set
of atoms, respectively.

Arithmetic expressions can be made out of other expressions by
applying various arithmetic operators, e.g. ``Issue.owner.name =
'Alice Smith'``. Because it is used in the ``FILTER`` clause, the
expression is evaluated for every member of the ``SELECT`` set and
used to filter out some of these members from the result.

.. code-block:: eql

    WITH MODULE example
    SELECT (
        SELECT Issue
        FILTER Issue.owner.name = 'Alice Smith'
    ).time_estimate;

The above query will return a set of time estimates for all of the
issues owned by Alice Smith rather than the ``Issue`` objects.

.. note::

    ``time_estimate`` is an *atomic value* (integer), so the resulting
    set can contain duplicate values. Every integer is effectively
    considered a distinct element of the set even when there are
    already set elements of the same value in the set. See
    :ref:`Everything is a set<ref_overview_set>` and
    :ref:`how expressions work<ref_edgeql_expressions>` for more
    details.

.. code-block:: eql

    WITH MODULE example
    SELECT (Issue.name, Issue.body)
    FILTER Issue.owner.name = 'Alice Smith';

The above query will return a set of 2-tuples containing the values of issue
``name`` and ``body`` for all of the issues owned by Alice Smith.
:ref:`Tuples<ref_edgeql_types_tuples>` can be used in other
expressions as a whole opaque entity or serialized for some external
use. This construct is similar to selecting individual columns in SQL
except that the column name is lost. If structural information is
important *shapes* should be used instead.


.. include:: paths.rst


.. _ref_edgeql_shapes:

Shapes
------

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
        # "issues" is not a link in the schema, it is a computable
        # defined in the shape
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


Using shapes
------------

:ref:`Shapes<ref_edgeql_shapes>` are the way of specifying structured
object data. They are used to get not only a set of *objects*, but
also a set of their relationships in a structured way. Shape
specification can be added to any expression that denotes an object.
Fundamentally, a shape specification does not alter the identity of
the objects it is attached to, because it doesn't in any way change
the existing objects, but rather specifies additional data about them.

For example, a query that retrieves a set of ``Issue`` objects with
``name`` and ``body``, but no other information (like
``time_estimate``, ``owner``, etc.) for all of the issues owned by
Alice Smith, would look like this:

.. code-block:: eql

    WITH MODULE example
    SELECT
    Issue {
        name,
        body
    } FILTER Issue.owner.name = 'Alice Smith';

Shapes can be nested to retrieve more complex structures:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue {  # base shape
        name,
        body,
        owner: {    # this is a nested shape
            name
        }
    };

The above query will retrieve all of the ``Issue`` objects. Each
object will have ``name``, ``body`` and ``owner`` links, where
``owner`` will also have a ``name``. To restrict this to only issues
that are not 'closed', the following query can be used:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue {  # base shape
        name,
        body,
        owner: {    # this is a nested shape
            name
        }
    } FILTER Issue.status.name != 'closed';


To retrieve all users and their associated issues (if any), the following
shape query can be used:

.. code-block:: eql

    WITH MODULE example
    SELECT User {
        name,
        <owner: Issue {
            name,
            body,
            status: {
                name
            }
        }
    };

The entry ``<owner`` indicates an inbound link named ``owner`` should
be followed to its origin. The shape of the origin for owner must be
that of an ``Issue`` (this is similar to ``User.<owner[IS Issue]``
:ref:`path<ref_edgeql_paths>`). By default links referred to in shapes
are considered to be outbound (like link ``status`` for the concept
``Issue``). Since the link ``owner`` on ``Issue`` is ``*1`` (by
default), when it is followed in the other direction is functions as a
``1*``. So ``<owner`` points to a `set` of multiple issues sharing a
particular owner. For each issue the sub-shape for the ``status`` link
will be retrieved containing just the ``name``.

Note that the the sub-shape does not mandate that only the users that
*own* at least one ``Issue`` are returned, merely that *if* they have
some issues the names and bodies of these issues should be included in
the returned value. The query effectively says 'please return the set
of *all* users and provide this specific information for each of them
if available'. This is one of the important differences between
*shape* specification and a :ref:`path<ref_edgeql_paths>`.
