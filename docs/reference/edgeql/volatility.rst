.. _ref_reference_volatility:


Volatility
==========

The **volatility** of an expression refers to how its value may change across
successive evaluations. 

Expressions may have one of the following volatilities, in order of increasing
volatility:

* ``Immutable``: The expression cannot modify the database and is
    guaranteed to have the same value *in all statements*.

* ``Stable``: The expression cannot modify the database and is
    guaranteed to have the same value *within a single statement*.

* ``Volatile``: The expression cannot modify the database and can have
    different values on successive evaluations.

* ``Modifying``: The expression can modify the database and can have
    different values on successive evaluations.


Expressions
-----------

All :ref:`primitives <ref_datamodel_primitives>`,
:ref:`ranges <ref_std_range>`, and
:ref:`multiranges <ref_std_multirange>` are ``Immutable``.

:ref:`Arrays <ref_std_array>`, :ref:`tuples <ref_std_tuple>`, and
:ref:`sets <ref_eql_sets>` have the volatility of their most volatile
component.

:ref:`Globals <ref_datamodel_globals>` are always ``Stable``, even computed
globals with an immutable expression.


Objects and shapes
^^^^^^^^^^^^^^^^^^

:ref:`Objects <ref_datamodel_object_types>` are generally ``Stable`` except:

* Objects with a :ref:`shape <ref_eql_shapes>` containing a more volatile
  computed pointer will have the volatility of its most volatile component.

* :ref:`Free objects <ref_eql_select_free_objects>` have the volatility of
  their most volatile component. They may be ``Immutable``.

An object's non-computed pointers are ``Stable``. Its computed pointers have
the volatility of their expressions.

Any DML (i.e., :ref:`insert <ref_eql_insert>`, :ref:`update <ref_eql_update>`,
:ref:`delete <ref_eql_delete>`) is ``Modifying``.


Functions and operators
^^^^^^^^^^^^^^^^^^^^^^^

Unless explicitly specified, a :ref:`function's <ref_eql_sdl_functions>`
volatility will be inferred from its body expression.

A function call's volatility is highest of its body expression and its call
arguments.

Given:

.. code-block:: sdl

    # Immutable
    function plus_primitive(x: float64) -> float64
        using (x + 1);

    # Stable
    global one := 1;
    function plus_global(x: float64) -> float64
        using (x + one);

    # Volatile
    function plus_random(x: float64) -> float64
        using (x + random());

    # Modifying
    type One {
        val := 1;
    };
    function plus_insert(x: float64) -> float64
        using (x + (insert One).val);

Some example operator and function calls:

.. code-block::

    1 + 1:                    Immutable
    1 + global one:           Stable
    global one + random():    Volatile
    (insert One).val:         Modifying
    plus_primitive(1):        Immutable
    plus_stable(1):           Stable
    plus_random(global one):  Volatile
    plus_insert(random()):    Immutable


Restrictions
------------

Some features restrict the volatility of expressions. A lower volatility
can be used.

:ref:`Indexes <ref_datamodel_indexes>` expressions must be ``Immutable``.
Within the index, pointers to the indexed object are treated as immutable

:ref:`constraints <ref_datamodel_constraints>` expressions must be
``Immutable``. Within the constraint, the ``__subject__`` and its pointers are
treated as immutable.

:ref:`Access policies <ref_datamodel_access_policies>` must be ``Stable``.

:ref:`Aliases <ref_eql_ddl_aliases>`, :ref:`globals <ref_datamodel_globals>`,
and :ref:`computed pointers <ref_datamodel_computed>` in the schema must be
``Stable``.

The :ref:`cartesian product <ref_reference_cardinality_cartesian>` of a
``Volatile`` or ``Modifying`` expression is not allowed.

.. code-block:: edgeql-repl

    db> SELECT {1, 2} + random()
    QueryError: can not take cross product of volatile operation

``Modifying`` expressions are not allowed in a non-scalar argument to a
function, except for :ref:`standard set functions <ref_std_set>`.

The non-optional parameters of ``Modifying``
:ref:`functions <ref_datamodel_functions_modifying>` must have a
:ref:`cardinality <ref_reference_cardinality>` of ``One``. Optional
parameters must have a cardinality of ``AtMostOne``.
