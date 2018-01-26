.. _ref_edgeql_fundamentals:


Fundamentals
============

EdgeQL is the query language used to work with EdgeDB and it has been
designed to work with objects, their properties and relations. It is a
declarative functional statically typed language. Conceptually every
query can be broken down into a composition of several functions
taking some subset of the whole data as input and producing another
set of data. The various expressions and clauses correspond to
specific functions in this composition. EdgeQL is structured
syntactically in blocks called clauses. The first clause takes
original data as input and every subsequent clause takes the output of
the previous clause as its input. In this way it is fairly easy to
keep track of what a given query is trying to express by following
along this sequence of transformations.

Data
----

The data in EdgeDB forms a directed labeled graph. The nodes contain
the data, while the edges represent links. The schema is a formal
description of all of the legal data types and link types.

.. insert sample schema and data graph here

.. _ref_edgeql_fundamentals_paths:

Paths
+++++

Consider a path through the data graph. The path effectively
represents a mapping of the starting node onto the target node. An
EdgeQL `path expression` represents a set of such paths. For example,
``Issue.owner`` is a path expression that represents a set of paths
that start at all of the ``Issue`` nodes and follow the edges from the
``owner`` set. Path expressions typically start with a `concept` (e.g.
``Issue``) defining a set of starting nodes. Then a ``.``-separated
sequence of links that are legally reachable according to the schema
determines the sequence of edges that must be followed (e.g.
``Issue.owner`` and ``Issue.owner.email`` are legal path expressions,
but ``Issue.email`` is not). Path expressions themselves represent a
valid set of nodes (the end-nodes of all the paths in the data graph).
So Path expressions evaluate to the collection of values contained in
the set of target nodes. Note that every path is the set denoted by
the path expression **must** include every edge specified by the
links, no partial paths are allowed.

In EdgeQL we use path expressions to represent the set of target nodes
(we also treat ``Issue`` as a trivial 0-edge path where the target set
is the same as the starting set). Every path expression is a set
function that maps a set of nodes onto another set of nodes reachable
via graph edges.

.. note::

    For brevity, this documentation refers to a `path expression` as
    simply a `path` everywhere else. In the rare instances when
    disambiguation is needed, *data graph path* and *path expression*
    is used explicitly.

    Similarly, the `value` of a `path` is intended to mean the
    collection of values of the set of target nodes.

    The first element of a `path` is often called its `root`.

.. _ref_edgeql_fundamentals_same:

There's also a basic principle in EdgeQL that *the same symbol refers
to the same thing* (in absence ``DETACHED`` keyword). This is fairly
intuitive for simple expressions involving paths with a common prefix
(shared symbol) such as:

.. code-block:: eql

    WITH MODULE example
    SELECT (User.first_name, User.last_name);

The query, in fact, does select a set of tuples containing first and
last names of each user. The path prefix ``User`` refers to the same
entity in both parts of the expression. Typically this property makes
it easier to write concise queries without having to worry about
accidentally introducing a cross-product from all possible
combinations.

For a complete description of paths refer to
:ref:`this section<ref_edgeql_fundamentals_path>`.

Shapes
++++++

Shapes are a way to specify entire sets of trees in the data graph.
The first element of the shape is the `root` of the tree. The nested
structure consists of various legally reachable links.

.. code-block:: eql

    WITH MODULE example
    SELECT
        # everything below is a shape
        Issue {  # root
            number,
            owner: {  # sub-shape
                name,
                email
            }
        };

One big difference between shapes and path expressions is that any
non-root shape element is optional. This means that every tree denoted
by a shape must start at the shape's root and be the largest reachable
tree given the hierarchy of links in the shape.

For a complete description of shapes refer to
:ref:`this section<ref_edgeql_shapes>`.


.. _ref_edgeql_fundamentals_multisets:

Multisets
+++++++++

Every EdgeQL expression evaluates to a non-nested `multiset` (a set
that allows duplicate elements) of some data. However, it's worth
nothing that some of the basic building blocks always produce `sets`
(i.e. they guarantee that there are no duplicate elements).
Specifically paths that end with a link which is pointing to a
`concept` (e.g. ``Issue.owner``) always produce sets of unique
objects. This is due to 2 properties of EdgeDB:

1) No two data graph nodes contain the same `object`.

2) The value of a path is given by the collection of the values within
   the `set` of target data graph nodes.

So because a path expression denotes a set of unique data graph nodes,
it follows that it also evaluates to a unique set of `objects` from
those nodes.

This is not true for a path targeting an `atom` (or any non-concept).
Multiple graph nodes are allowed to contain the same atomic value. So
a path like ``Issue.time_estimate`` could evaluate to a `multiset`
with duplicates.

.. note::

    For the sake of brevity, this documentation uses the word `sets`
    when referring to expression values. It is to be understood that
    in the general case this implies a `multiset` instead. When a
    disambiguation is necessary the uniqueness of elements is
    explicitly addressed.


.. _ref_edgeql_fundamentals_function:

Functions
---------

EdgeQL is a functional language in the sense that every query
expression can be represented as a composition of set functions. So
every clause and operator in EdgeQL are conceptually equivalent to
some set function. User-defined functions also follow the same base
principles.

A set function takes zero or more sets as input and produces a set as
output. For simplicity, consider a set function with only one input
parameter. Any given input set can be handled by the function in one
of the following ways:

- Element-wise.

  The output set can be derived by applying the same function to each
  individual input element (taken as a singleton) and merging the
  result with a union. This element-wise nature of a function is
  typical of basic arithmetic
  :ref:`operators<ref_edgeql_expressions_elops>`. This is also the
  default for user-defined functions in EdgeQL.

  .. code-block:: eschema

    # schema definition of a function that will be
    # applied in an element-wise fashion
    function plus_ten(int) -> int:
        from edgeql :>
            SELECT $0 + 10;

  In the above example only the input type without any additional
  qualifiers is given. This means that the function will be
  interpreted as an element-wise function. In particular this means
  that it will *not* be called on empty sets, since the result of any
  element-wise function applied to an empty set is an empty set.

- Element-wise with special handling of the empty set.

  For non-empty inputs the output set is produced exactly the same way
  as for a regular element-wise case. However, the function will be
  invoked for empty set input as well since it may produce some
  special output even in this case.

  .. code-block:: eschema

    # schema definition of a function that will be
    # applied in an element-wise fashion with special
    # handling of empty input
    function plus_ten2(optional int) -> int:
        from edgeql :>
            SELECT $0 + 10 IF EXISTS $0 ELSE 10;

  The above example works just like ``plus_ten``, but in addition
  produces the result of ``10`` even when the input is an empty set.
  Note that without the ``optional`` keyword ``plus_ten2`` would be
  functionally identical to ``plus_ten`` as it would never be invoked
  on empty input (regardless of the fact that it is capable of
  producing a non-empty result for it).

  This type of input handling is used by many EdgeQL operators. For
  example, it is used by the
  :ref:`coalescing operator<ref_edgeql_expressions_elops>` ``??``.

- Set as a whole.

  The output set is somehow dependent on the entire input set and
  cannot be produced by merging outputs in an element-wise fashion.
  This is typical of
  :ref:`aggregate functions<ref_edgeql_functions_count>`, such as
  ``sum`` or ``count``.

  .. code-block:: eschema

    # schema definition of a function that will be
    # applied to the input set as a whole
    function conatins_ten(set of int) -> bool:
        from edgeql :>
            SELECT 10 IN $0;

  The keywords ``set of`` mean that the input set works as a single
  entity. The output set for ``contains_ten`` is always a boolean
  singleton (either ``{TRUE}`` or ``{FALSE}``) and is independent of
  the input size.

It is important to note that these are technically properties of
function `parameters` and not the function overall. It is perfectly
possible to have a function that behaves in an element-wise fashion
w.r.t. one parameter and is aggregate-like w.r.t. another. In fact,
the EdgeQL :ref:`operator<ref_edgeql_expressions>` ``IN`` has exactly
this property.

There's another important interaction of function arguments. As long
as the arguments are independent of each other (i.e. they use
different symbols) the qualifiers in the function definition govern
how the function is applied as per the above. However, if the
arguments are dependent (i.e. they use the same symbols) then there's
an additional rule to resolve how the function is applied:

.. note::

    If even one of the arguments is element-wise, all arguments that
    are related to it must behave in an element-wise fashion
    regardless of the qualifiers.

This rule basically takes the principle that ":ref:`the same symbol
refers to the same thing<ref_edgeql_fundamentals_same>`" and applies
it to the function arguments. That's why if some symbol is interpreted
as an element-wise argument then it must be element-wise for all other
arguments of the same function.

Consider the following query:

.. code-block:: eql

    # the signature of built-in 'count':
    # function count(SET OF any) -> int

    WITH MODULE example
    SELECT count(Issue.watchers);

The function ``count`` normally treats the argument set as a whole, so
the query above counts the total number of distinct issue watchers. To
get a count of issue watchers on a per-issue basis, the following
query is needed:

.. code-block:: eql

    WITH MODULE example
    SELECT (Issue, count(Issue.watchers));

Tuples behave like element-wise functions w.r.t. all of their
elements. This means that the symbol ``Issue`` is treated as an
element-wise argument in this context. This, in turn, means that it
``count`` is evaluated separately for each element of ``Issue``. So
the result is a set of tuples containing an issue and a watchers count for
that specific issue much like the simpler example of :ref:`user
name<ref_edgeql_fundamentals_same>`.


.. _ref_edgeql_fundamentals_scope:

Scope
-----

.. this section is going to need some more coherence

Scoping rules build on top of another rule: same symbol means the same
thing (in particular that means that same path prefixes mean the same
thing anywhere in the expression). Scoping rules specify when the same
symbols may refer to *different* entities. So the full rule can be
stated as follows:

.. note::

    Same symbols mean the same thing within any specific scope.

Every EdgeQL statement exists in its own scope. One can also envision
the current state of the DB as a base scope (or schema-level scope)
within which statements are defined. This schema-level scope notion is
relevant for understanding how ``DETACHED`` keyword works.

What creates a new scope? Any time a function with a ``SET OF``
argument is called, that argument exists in its own sub-scope (or
nested scope). Any nested scope is affected by all the enclosing
scopes, but any further refinement of a symbol's semantics do not
propagate back up. This also means that parallel (or sibling) scopes
do not affect each other's semantics.

.. code-block:: eql

    # Select first and last name for each user.
    WITH MODULE example
    SELECT (User.first_name,
            # this mention of 'User' is the same
            # as the one above
            User.last_name);

    # Select the counts of first and last names.
    # This is kind of trivial, but
    WITH MODULE example
    SELECT (
        # The argument to 'count' exists in its own sub-scope.
        # User.first_name and User.last_name in that sub-scope are
        # treated element-wise.
        count(User.first_name + User.last_name),

        # The argument to 'count' exists in a different sub-scope.
        # User.email in this sub-scope is not related to the
        # User.last_name above.
        count(User.email)
    );

Due to parallel sub-scopes, both ``count`` expressions are evaluated
on the input sets as a whole and not on a per-user basis like in a
tuple.

The ``DETACHED`` keyword creates a whole new scope, parallel to the
statement in which it appears, nested directly in the schema-level
scope.

.. code-block:: eql

    # select first and last name for each user
    WITH MODULE example
    SELECT (User.first_name,
            # this mention of 'User' is the same
            # as the one above
            User.last_name);

    # select all possible combinations of first and last names
    WITH MODULE example
    SELECT (User.first_name,
            # DETACHED keyword makes this mention of 'User'
            # completely unrelated to the one above
            DETACHED User.last_name);

One way to interpret any query is to follow these steps:

1) Lexically substitute any aliases recursively until no aliases are used.

2) Find all ``DETACHED`` expressions and treat them as entirely
   separate from anything else within the statement. One way to think
   of this is to imagine that there's actually a schema-level view
   defined for each of the ``DETACHED`` expressions.

3) Resolve whether each particular function will be evaluated element-
   wise or not based on the ``SET OF`` scoping rules.


.. _ref_edgeql_fundamentals_path:

.. potentially this section should be moved into operators since it
   covers `.`, `.>`, `.<`, `[IS ...]`, and `@`

Path Expressions
----------------

Path expressions (typically referred to as simply `paths`) are
fundamental building blocks of EdgeQL. A path defines a set of data in
EdgeDB (just like any other expression) based on the data type and
relationship with other data.

A path always starts with some ``concept`` as its `root` and it may
have an arbitrary number of `steps` following various ``links``. The
simplest path consists only of a `root` and is interpreted to mean
'all objects of the type `root`'.

.. code-block:: eql

    WITH MODULE example
    SELECT Issue;

In the above example ``Issue`` is a path that represents all objects in
the database of type ``Issue``. That is the result of the above query.

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.owner;

The path ``Issue.owner`` consists of the `root` ``Issue`` and a `path
step` ``.owner``. It specifies the set of all objects that can be
reached from any object of type ``Issue`` by following its link
``owner``. This means that the above query will only retrieve users
that actually have at least one issue. The ``.`` operator in the path
separates `steps` and each step corresponds to a ``link`` name that
must be followed. By default, links are followed in the `outbound`
direction (the direction that is actually specified in the schema).
The direction of the link can be also specified explicitly by using
``>`` for `outbound` and ``<`` for `inbound`. Thus, the above query can be
rewritten more explicitly, but equivalently as:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.>owner;

To select all issues that actually have at least one watcher, it is
possible to construct a path using `inbound` link:

.. code-block:: eql

    WITH MODULE example
    SELECT User.<watchers;

The path in the above query specifies the set of all objects that can
be reached from ``User`` by following any ``link`` named ``watchers``
that has ``User`` as its target, back to the source of the ``link``.
In our case, there is only one link in the schema that is called
``watchers``. This link belongs to ``Issue`` and indeed it has
``User`` as its target, so the above query will get all the ``Issue``
objects that have at least one watcher. Only links that have a concept
as their target can be followed in the `inbound` direction. It is not
possible to follow inbound links on atoms.

Just like the direction of the step can be specified explicitly in a
path, so can the type of the link target. In order to retrieve all the
``SystemUsers`` that have actually created new ``Issues`` (as opposed
to ``Comments``) the following query could be made:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.owner[IS SystemUser];

In the above query the `path step` is expressed as ``owner[IS
SystemUser]``, where ``owner`` is the name of the link to follow, and
the qualifier ``[IS ...]`` specifies a restriction on the target's
type.

This is equivalent to:

.. code-block:: eql

    WITH MODULE example
    SELECT Issue.owner
    FILTER Issue.owner IS SystemUser;

The biggest difference between the two of the above representations is
that ``[IS SystemUser]`` allows to refer to links specific to
``SystemUser``.

Finally combining all of the above, it is possible to write a query to
retrieve all the ``Comments`` to ``Issues`` created by ``SystemUsers``:

.. code-block:: eql

    WITH MODULE example
    SELECT SystemUser.<owner[IS Issue].<issue;

    # or equivalently

    WITH MODULE example
    SELECT SystemUser
        # follow the link 'owner' to a source Issue
        .<owner[IS Issue]
        # follow the link 'issue' to a source Comment
        .<issue[IS Comment];

.. note::

    Links technically also belong to a module. Typically, the module
    doesn't need to be specified (because it is the default module or
    the link name is unambiguous), but sometimes it is necessary to
    specify the link module explicitly. The entire fully-qualified
    link name then needs to be enclosed in parentheses:

    .. code-block:: eql

        WITH MODULE some_module
        SELECT A.(another_module::foo).bar;

Link properties
+++++++++++++++

It is possible to have a path that represents a set of link properties
as opposed to link target values. Since link properties have to be
atomic, the step pointing to the link property is always the last step
in a path. The link property is accessed by using ``@`` instead
of ``.``.

Consider the following schema:

.. code-block:: eschema

    link favorites:
        link property rank to int

    concept Post:
        required link body to str
        required link owner to User

    concept User extending std::Named:
        link favorites to Post:
            mapping := '**'

Then the query selecting all favorite Post sorted by their rank is:

.. code-block:: eql

    WITH MODULE example
    SELECT User.favorites
    ORDER BY User.favorites@rank;


.. THE BELOW IS STILL IN PROCESS OF REWRITING

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
important `shapes` should be used instead.


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
represent that set as a `forest`, where each base object is the root
of a `tree`. Technically, this set of trees is a directed graph
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
object data. They are used to get not only a set of `objects`, but
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
`shape` specification and a :ref:`path<ref_edgeql_paths>`.
