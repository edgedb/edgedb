.. _ref_edgeql_overview:


Overview
========

EdgeQL is the query language used to work with EdgeDB and it has been
designed to work with objects, their properties and relations. For the
purposes of this section we will use the following schema that is
assumed to be part of the module ``example``:

.. code-block:: eschema

    abstract concept Text:
        # This is an abstract object containing text.
        required link body to str:
            constraint maxlength:
                # Maximum length of text is 10000
                # characters.
                10000

    concept User extends std::Named
    # NamedObject is a standard abstract base class,
    # that provides a name link.

    concept SystemUser extends std::User
    # a type of user that represents various automatic systems, that
    # might add comments to issues, perhaps based on some automatic
    # escalation system for unresolved higher priority issues

    abstract concept Owned:
        # By default links are optional.
        required link owner to User

    concept Status extends std::Dictionary
    # Dictionary is a NamedObject variant, that enforces
    # name uniqueness across all instances if its subclass.

    concept Priority extends std::Dictionary

    concept LogEntry extends OwnedObject, Text:
        # LogEntry is an OwnedObject and a Text, so it
        # will have all of their links and attributes,
        # in particular, owner and text links.
        required link spent_time to int

    atom issue_num_t extends std::sequence
    # issue_num_t is defined as a concrete sequence type,
    # used to generate sequential issue numbers.

    concept Comment extends Text, Owned:
        required link issue to Issue
        link parent to Comment

    concept Issue extends std::Named, Owned, Text:

        required link number to issue_num_t:
            readonly: true
            # The number values are automatically generated,
            # and are not supposed to be directly writable.

        required link status to Status

        link priority to Priority

        link watchers to User:
            mapping: **
            # The watchers link is mapped to User concept in
            # many-to-many relation.  The default mapping is
            # *1 -- many-to-one.

        link time_estimate to int

        link time_spent_log to LogEntry:
            mapping: 1*
            # 1* -- one-to-many mapping.

        link start_date to datetime:
            default := SELECT datetime::current_datetime()
            # The default value of start_date will be a
            # result of the EdgeQL expression above.

        link due_date to datetime

        link related_to to Issue:
            mapping: **

This schema represents the data model for an issue tracker. There
are ``Users``, who can create an ``Issue``, add a ``Comment`` to an
``Issue``, or add a ``LogEntry`` to document work on a particular
``Issue``. ``Issues`` can be related to each other. A ``User`` can
watch any ``Issue``. Every ``Issue`` has a ``Status`` and possibly a
``Priority``.

The general structure of a simple EdgeQL query:

.. code-block:: none

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
Expressions in all query clauses act as path generators. ``FILTER``
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
objects or values. Basically, anything with an ``id`` is an object and
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
    SELECT Issue.time_estimate
    FILTER Issue.owner.name = 'Alice Smith';

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
important *shapes* should be used instead. So selecting tuples is
quite rare in EdgeQL and is not encouraged.


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
