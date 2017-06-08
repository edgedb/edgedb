Core features of EdgeDB
=======================

Everything is a set
-------------------

EdgeDB is fundamentally working with sets, unlike SQL. Which means
that there can be no duplicate results. The *identity* of an object is
determined by its ``id``. For practical reasons there's a caveat for
atomic values, whose identity is defined as being *always* unique
(essentially every instance of the atomic value is a different unique
entity as far as sets are concerned). There are a few special
interactions with :ref:`set operators<ref_edgeql_expressions_setops>`
that are worth noting.

All sets must also be homogeneous, i.e. all members of a set have to
be of the same basic :ref:`type<ref_edgeql_types>`. Thus all sets are
either composed of *objects*, *atomic values* or *tuples*. It's worth
noting that mixing objects representing different
:ref:`concepts<ref_schema_architechture_concepts>` is fine
since they are all derived from the same base ``Object``.

For more details see :ref:`how expressions work<ref_edgeql_expressions>`.


Why EMPTY?
----------

Traditional relational DBs deal with tables and use ``NULL`` as a
value denoting absence of data. Thus ``NULL`` is a special *value* in
those DBs. EdgeDB works with *sets*, so when a link/relationship is
missing, there is no actual value associated with it. That's why we
chose to work in terms of sets and ``EMPTY`` is the set representing a
non-existent entity.

One of the consequences of this is that DBs that support ``NULL``
basically operate on 3 logical values: ``TRUE``, ``NULL``, ``FALSE``.
This results in complicated and non-Boolean logic. By getting rid of
``NULL`` as a *value*, we make sure that EdgeDB operated only on
Boolean ``TRUE`` and ``FALSE`` logical values. This has important
consequences for how query results get filtered.


.. todo::

    (insert diagram of 2 users with and without a profile)


How do we work with EMPTY?
--------------------------

``EMPTY`` is not a value, so it will never show up in results as such.
If it is desirable to compute a relationship for some objects and that
relationship can be ``EMPTY``, but it is desirable to still list those
objects a *shape* is well-suited to do exactly that.
