.. _ref_eql_functions_math:


====
Math
====

:edb-alt-title: Mathematical Functions


.. list-table::
    :class: funcoptable

    * - :eql:func:`math::abs`
      - :eql:func-desc:`math::abs`

    * - :eql:func:`math::ceil`
      - :eql:func-desc:`math::ceil`

    * - :eql:func:`math::floor`
      - :eql:func-desc:`math::floor`

    * - :eql:func:`math::ln`
      - :eql:func-desc:`math::ln`

    * - :eql:func:`math::lg`
      - :eql:func-desc:`math::lg`

    * - :eql:func:`math::log`
      - :eql:func-desc:`math::log`

    * - :eql:func:`math::mean`
      - :eql:func-desc:`math::mean`

    * - :eql:func:`math::stddev`
      - :eql:func-desc:`math::stddev`

    * - :eql:func:`math::stddev_pop`
      - :eql:func-desc:`math::stddev_pop`

    * - :eql:func:`math::var`
      - :eql:func-desc:`math::var`

    * - :eql:func:`math::var_pop`
      - :eql:func-desc:`math::var_pop`


-----------


.. eql:function:: math::abs(x: anyreal) -> anyreal

    :index: absolute

    Return the absolute value of the input *x*.


    .. code-block:: edgeql-repl

        db> SELECT math::abs(1);
        {1}
        db> SELECT math::abs(-1);
        {1}


----------


.. eql:function:: math::ceil(x: int64) -> float64
                  math::ceil(x: float64) -> float64
                  math::ceil(x: bigint) -> bigint
                  math::ceil(x: decimal) -> decimal

    :index: round

    Round up to the nearest integer.

    .. code-block:: edgeql-repl

        db> SELECT math::ceil(1.1);
        {2}
        db> SELECT math::ceil(-1.1);
        {-1}


----------


.. eql:function:: math::floor(x: int64) -> float64
                  math::floor(x: float64) -> float64
                  math::floor(x: bigint) -> bigint
                  math::floor(x: decimal) -> decimal

    :index: round

    Round down to the nearest integer.

    .. code-block:: edgeql-repl

        db> SELECT math::floor(1.1);
        {1}
        db> SELECT math::floor(-1.1);
        {-2}


----------


.. eql:function:: math::ln(x: int64) -> float64
                  math::ln(x: float64) -> float64
                  math::ln(x: decimal) -> decimal

    :index: logarithm

    Return the natural logarithm of the input value.

    .. code-block:: edgeql-repl

        db> SELECT 2.718281829 ^ math::ln(100);
        {100.00000009164575}


----------


.. eql:function:: math::lg(x: int64) -> float64
                  math::lg(x: float64) -> float64
                  math::lg(x: decimal) -> decimal

    :index: logarithm

    Return the base 10 logarithm of the input value.

    .. code-block:: edgeql-repl

        db> SELECT 10 ^ math::lg(42);
        {42.00000000000001}

----------


.. eql:function:: math::log(x: decimal, NAMED ONLY base: decimal) -> decimal

    :index: logarithm

    Return the logarithm of the input value in the specified *base*.

    .. code-block:: edgeql-repl

        db> SELECT 3 ^ math::log(15n, base := 3n);
        {15.0000000000000005n}


----------


.. eql:function:: math::mean(vals: SET OF int64) -> float64
                  math::mean(vals: SET OF float64) -> float64
                  math::mean(vals: SET OF decimal) -> decimal

    :index: average avg

    Return the arithmetic mean of the input set.

    .. code-block:: edgeql-repl

        db> SELECT math::mean({1, 3, 5});
        {3}


----------


.. eql:function:: math::stddev(vals: SET OF int64) -> float64
                  math::stddev(vals: SET OF float64) -> float64
                  math::stddev(vals: SET OF decimal) -> decimal

    :index: average

    Return the sample standard deviation of the input set.

    .. code-block:: edgeql-repl

        db> SELECT math::stddev({1, 3, 5});
        {2}

.. eql:function:: math::stddev_pop(vals: SET OF int64) -> float64
                  math::stddev_pop(vals: SET OF float64) -> float64
                  math::stddev_pop(vals: SET OF decimal) -> decimal

    :index: average

    Return the population standard deviation of the input set.

    .. code-block:: edgeql-repl

        db> SELECT math::stddev_pop({1, 3, 5});
        {1.63299316185545}


----------


.. eql:function:: math::var(vals: SET OF int64) -> float64
                  math::var(vals: SET OF float64) -> float64
                  math::var(vals: SET OF decimal) -> decimal

    :index: average

    Return the sample variance of the input set.

    .. code-block:: edgeql-repl

        db> SELECT math::var({1, 3, 5});
        {4}


----------


.. eql:function:: math::var_pop(vals: SET OF int64) -> float64
                  math::var_pop(vals: SET OF float64) -> float64
                  math::var_pop(vals: SET OF decimal) -> decimal

    :index: average

    Return the population variance of the input set.

    .. code-block:: edgeql-repl

        db> SELECT math::var_pop({1, 3, 5});
        {2.66666666666667}
