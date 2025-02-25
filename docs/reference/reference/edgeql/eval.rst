.. _ref_eql_fundamentals_queries:

====================
Evaluation algorithm
====================

EdgeQL is a functional language in the sense that every expression is
a composition of one or more queries.

Queries can be *explicit*, such as a :eql:stmt:`select` statement,
or *implicit*, as dictated by the semantics of a function, operator or
a statement clause.

An implicit ``select`` subquery is assumed in the following situations:

- expressions passed as an argument for an aggregate function parameter
  or operand;

- the right side of the assignment operator (``:=``) in expression
  aliases and :ref:`shape element declarations <ref_reference_shapes>`;

- the majority of statement clauses.

A nested query is called a *subquery*.  Here, the phrase
"*apearing directly in the query*" means
"appearing directly in the query rather than in the subqueries".

.. _ref_eql_fundamentals_eval_algo:

A query is evaluated recursively using the following procedure:

1. Make a list of simple paths (i.e., paths that begin with a set reference)
   appearing directly the query.  For every path in the list, find all paths
   which begin with the same set reference and treat their longest common
   prefix as an equivalent set reference.

   Example:

   .. code-block:: edgeql

      select (
        User.firstname,
        User.friends.firstname,
        User.friends.lastname,
        Issue.priority.name,
        Issue.number,
        Status.name
      );

   In the above query, the longest common prefixes are: ``User``,
   ``User.friends``, ``Issue``, and ``Status.name``.

2. Make a *query input list* of all unique set references which appear
   directly in the query (including the common path prefixes identified above).
   The set references and path prefixes in this list are called *input
   set references*,  and the sets they represent are called *input
   sets*. Order this list such that any input references come before
   any other input set reference for which it is a prefix (sorting
   lexicographically works).

3. Compute a set of *input tuples*.

   - Begin with a set containing a single empty tuple.
   - For each input set reference, we compute a *dependent* Cartesian
     product of the input tuple set (``X``) so far and the input set
     ``Y`` being considered. In this dependent product, we pair each
     tuple ``x`` in the input tuple set ``X`` with each element of the
     subset of the input set ``Y`` corresponding to the tuple ``x``. (For
     example, in the above example, computing the dependent product
     of User and User.friends would pair each user with all of their
     friends.)

     (Mathematically, ``X' = {(x, y) | x ∈ X, y ∈ f(x)}``, if ``f(x)``
     selects the appropriate subset.)

     The set produced becomes the new input tuple set and we continue
     down the list.
   - As a caveat to the above, if an input set appears exclusively as
     an :ref:`optional <ref_sdl_function_typequal>` argument, it produces
     pairs with a placeholder value ``Missing`` instead of an empty
     Cartesian product in the above
     set. (Mathematically, this corresponds to having ``f(x) =
     {Missing}`` whenever it would otherwise produce an empty set.)

4. Iterate over the set of input tuples, and on every iteration:

   - in the query and its subqueries, replace each input set reference with the
     corresponding value from the input tuple or an empty set if the value
     is ``Missing``;

   - evaluate the query expression in the order of precedence using
     the following rules:

     * subqueries are evaluated recursively from step 1;

     * a function or an operator is evaluated in a loop over a Cartesian
       product of its non-aggregate arguments
       (empty ``optional`` arguments are excluded from the product);
       aggregate arguments are passed as a whole set;
       the results of the invocations are collected to form a single set.

5. Collect the results of all iterations to obtain the final result set.
