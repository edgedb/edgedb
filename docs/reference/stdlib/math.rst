.. _ref_std_math:


====
Math
====

:edb-alt-title: Mathematical Functions

.. include:: math_funcops_table.rst

-----------


.. eql:function:: math::abs(x: anyreal) -> anyreal

    :index: absolute

    Returns the absolute value of the input.


    .. code-block:: edgeql-repl

        db> select math::abs(1);
        {1}
        db> select math::abs(-1);
        {1}


----------


.. eql:function:: math::ceil(x: int64) -> float64
                  math::ceil(x: float64) -> float64
                  math::ceil(x: bigint) -> bigint
                  math::ceil(x: decimal) -> decimal

    :index: round

    Rounds up a given value to the nearest integer.

    .. code-block:: edgeql-repl

        db> select math::ceil(1.1);
        {2}
        db> select math::ceil(-1.1);
        {-1}


----------


.. eql:function:: math::floor(x: int64) -> float64
                  math::floor(x: float64) -> float64
                  math::floor(x: bigint) -> bigint
                  math::floor(x: decimal) -> decimal

    :index: round

    Rounds down a given value to the nearest integer.

    .. code-block:: edgeql-repl

        db> select math::floor(1.1);
        {1}
        db> select math::floor(-1.1);
        {-2}


----------


.. eql:function:: math::ln(x: int64) -> float64
                  math::ln(x: float64) -> float64
                  math::ln(x: decimal) -> decimal

    :index: logarithm

    Returns the natural logarithm of a given value.

    .. code-block:: edgeql-repl

        db> select 2.718281829 ^ math::ln(100);
        {100.00000009164575}


----------


.. eql:function:: math::lg(x: int64) -> float64
                  math::lg(x: float64) -> float64
                  math::lg(x: decimal) -> decimal

    :index: logarithm

    Returns the base 10 logarithm of a given value.

    .. code-block:: edgeql-repl

        db> select 10 ^ math::lg(42);
        {42.00000000000001}

----------


.. eql:function:: math::log(x: decimal, named only base: decimal) -> decimal

    :index: logarithm

    Returns the logarithm of a given value in the specified base.

    .. code-block:: edgeql-repl

        db> select 3 ^ math::log(15n, base := 3n);
        {15.0000000000000005n}


----------


.. eql:function:: math::mean(vals: set of int64) -> float64
                  math::mean(vals: set of float64) -> float64
                  math::mean(vals: set of decimal) -> decimal

    :index: average avg

    Returns the arithmetic mean of the input set.

    .. code-block:: edgeql-repl

        db> select math::mean({1, 3, 5});
        {3}


----------


.. eql:function:: math::stddev(vals: set of int64) -> float64
                  math::stddev(vals: set of float64) -> float64
                  math::stddev(vals: set of decimal) -> decimal

    :index: average

    Returns the sample standard deviation of the input set.

    .. code-block:: edgeql-repl

        db> select math::stddev({1, 3, 5});
        {2}

.. eql:function:: math::stddev_pop(vals: set of int64) -> float64
                  math::stddev_pop(vals: set of float64) -> float64
                  math::stddev_pop(vals: set of decimal) -> decimal

    :index: average

    Returns the population standard deviation of the input set.

    .. code-block:: edgeql-repl

        db> select math::stddev_pop({1, 3, 5});
        {1.63299316185545}


----------


.. eql:function:: math::var(vals: set of int64) -> float64
                  math::var(vals: set of float64) -> float64
                  math::var(vals: set of decimal) -> decimal

    :index: average

    Returns the sample variance of the input set.

    .. code-block:: edgeql-repl

        db> select math::var({1, 3, 5});
        {4}


----------


.. eql:function:: math::var_pop(vals: set of int64) -> float64
                  math::var_pop(vals: set of float64) -> float64
                  math::var_pop(vals: set of decimal) -> decimal

    :index: average

    Returns the population variance of the input set.

    .. code-block:: edgeql-repl

        db> select math::var_pop({1, 3, 5});
        {2.66666666666667}


-----------


.. eql:function:: math::pi() -> float64

    :index: trigonometry

    Returns the value of pi.

    .. code-block:: edgeql-repl

        db> select math::pi();
        {3.141592653589793}


-----------


.. eql:function:: math::acos(x: float64) -> float64

    :index: trigonometry

    Returns the arc cosine of the input.

    .. code-block:: edgeql-repl

        db> select math::acos(-1);
        {3.141592653589793}
        db> select math::acos(0);
        {1.5707963267948966}
        db> select math::acos(1);
        {0}


-----------


.. eql:function:: math::asin(x: float64) -> float64

    :index: trigonometry

    Returns the arc sine of the input.

    .. code-block:: edgeql-repl

        db> select math::asin(-1);
        {-1.5707963267948966}
        db> select math::asin(0);
        {0}
        db> select math::asin(1);
        {1.5707963267948966}


-----------


.. eql:function:: math::atan(x: float64) -> float64

    :index: trigonometry

    Returns the arc tangent of the input.

    .. code-block:: edgeql-repl

        db> select math::atan(-1);
        {-0.7853981633974483}
        db> select math::atan(0);
        {0}
        db> select math::atan(1);
        {0.7853981633974483}


-----------


.. eql:function:: math::atan2(y: float64, x: float64) -> float64

    :index: trigonometry

    Returns the arc tangent of ``y / x``.

    Uses the signs of the arguments determine the correct quadrant.

    .. code-block:: edgeql-repl

        db> select math::atan2(1, 1);
        {0.7853981633974483}
        db> select math::atan2(1, -1);
        {2.356194490192345}
        db> select math::atan2(-1, -1);
        {-2.356194490192345}
        db> select math::atan2(-1, 1);
        {-0.7853981633974483}


-----------


.. eql:function:: math::cos(x: float64) -> float64

    :index: trigonometry

    Returns the cosine of the input.

    .. code-block:: edgeql-repl

        db> select math::cos(0);
        {1}
        db> select math::cos(math::pi() / 2);
        {0.000000000}
        db> select math::cos(math::pi());
        {-1}
        db> select math::cos(math::pi() * 3 / 2);
        {-0.000000000}


-----------


.. eql:function:: math::cot(x: float64) -> float64

    :index: trigonometry

    Returns the cotangent of the input.

    .. code-block:: edgeql-repl

        db> select math::cot(math::pi() / 4);
        {1.000000000}
        db> select math::cot(math::pi() / 2);
        {0.000000000}
        db> select math::cot(math::pi() * 3 / 4);
        {-0.999999999}


-----------


.. eql:function:: math::sin(x: float64) -> float64

    :index: trigonometry

    Returns the sinine of the input.

    .. code-block:: edgeql-repl

        db> select math::sin(0);
        {0}
        db> select math::sin(math::pi() / 2);
        {1}
        db> select math::sin(math::pi());
        {0.000000000}
        db> select math::sin(math::pi() * 3 / 2);
        {-1}


-----------


.. eql:function:: math::tan(x: float64) -> float64

    :index: trigonometry

    Returns the tanangent of the input.

    .. code-block:: edgeql-repl

        db> select math::tan(-math::pi() / 4);
        {-0.999999999}
        db> select math::tan(0);
        {0}
        db> select math::tan(math::pi() / 4);
        {0.999999999}
