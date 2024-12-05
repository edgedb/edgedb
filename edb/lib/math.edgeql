#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CREATE MODULE std::math;


CREATE FUNCTION
std::math::abs(x: std::anyreal) -> std::anyreal
{
    CREATE ANNOTATION std::description :=
        'Return the absolute value of the input *x*.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'abs';
};


CREATE FUNCTION
std::math::ceil(x: std::int64) -> std::int64
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT "x";';
};


CREATE FUNCTION
std::math::ceil(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT ceil("x");'
};


CREATE FUNCTION
std::math::ceil(x: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT "x";'
};


CREATE FUNCTION
std::math::ceil(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT ceil("x");'
};


CREATE FUNCTION
std::math::floor(x: std::int64) -> std::int64
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT "x";';
};


CREATE FUNCTION
std::math::floor(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT floor("x");';
};


CREATE FUNCTION
std::math::floor(x: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT "x";'
};


CREATE FUNCTION
std::math::floor(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'Immutable';
    USING SQL 'SELECT floor("x");';
};


CREATE FUNCTION
std::math::ln(x: std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the natural logarithm of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'ln';
};


CREATE FUNCTION
std::math::ln(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the natural logarithm of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'ln';
};


CREATE FUNCTION
std::math::ln(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the natural logarithm of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'ln';
};


CREATE FUNCTION
std::math::lg(x: std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the base 10 logarithm of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'log';
};


CREATE FUNCTION
std::math::lg(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the base 10 logarithm of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'log';
};


CREATE FUNCTION
std::math::lg(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the base 10 logarithm of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'log';
};


CREATE FUNCTION
std::math::log(x: std::decimal, NAMED ONLY base: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the logarithm of the input value in the specified *base*.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT log("base", "x")
    $$;
};


CREATE FUNCTION
std::math::sqrt(x: std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the square root of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sqrt';
};


CREATE FUNCTION
std::math::sqrt(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the square root of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sqrt';
};


CREATE FUNCTION
std::math::sqrt(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the square root of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sqrt';
};

# std::math::mean
# -----------
# The mean function returns an empty set if the input is empty set. On
# all other inputs it returns the mean for that input set.
CREATE FUNCTION
std::math::mean(vals: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the arithmetic mean of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'avg';
    SET error_on_null_result := 'invalid input to mean(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::mean(vals: SET OF std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the arithmetic mean of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'avg';
    # SQL 'avg' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to mean(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::mean(vals: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the arithmetic mean of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'avg';
    SET error_on_null_result := 'invalid input to mean(): not ' ++
                                'enough elements in input set';
};


# std::math::stddev
# ------------
CREATE FUNCTION
std::math::stddev(vals: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the sample standard deviation of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'stddev';
    SET error_on_null_result := 'invalid input to stddev(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::stddev(vals: SET OF std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample standard deviation of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'stddev';
    # SQL 'stddev' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to stddev(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::stddev(vals: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample standard deviation of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'stddev';
    SET error_on_null_result := 'invalid input to stddev(): not ' ++
                                'enough elements in input set';
};


# std::math::stddev_pop
# ----------------
CREATE FUNCTION
std::math::stddev_pop(vals: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the population standard deviation of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'stddev_pop';
    SET error_on_null_result := 'invalid input to stddev_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::stddev_pop(vals: SET OF std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population standard deviation of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'stddev_pop';
    # SQL 'stddev_pop' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to stddev_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::stddev_pop(vals: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population standard deviation of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'stddev_pop';
    SET error_on_null_result := 'invalid input to stddev_pop(): not ' ++
                                'enough elements in input set';
};


# std::math::var
# --------------
CREATE FUNCTION
std::math::var(vals: SET OF std::decimal) -> OPTIONAL std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the sample variance of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'variance';
    SET error_on_null_result := 'invalid input to var(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::var(vals: SET OF std::int64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample variance of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'variance';
    # SQL 'var' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to var(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::var(vals: SET OF std::float64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample variance of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'variance';
    SET error_on_null_result := 'invalid input to var(): not ' ++
                                'enough elements in input set';
};


# std::math::var_pop
# -------------
CREATE FUNCTION
std::math::var_pop(vals: SET OF std::decimal) -> OPTIONAL std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the population variance of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'var_pop';
    SET error_on_null_result := 'invalid input to var_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::var_pop(vals: SET OF std::int64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population variance of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'var_pop';
    # SQL 'var_pop' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to var_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::var_pop(vals: SET OF std::float64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population variance of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'var_pop';
    SET error_on_null_result := 'invalid input to var_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
std::math::pi() -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the constant value of pi.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'pi';
};


CREATE FUNCTION
std::math::acos(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the inverse cosine of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'acos';
};


CREATE FUNCTION
std::math::asin(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the inverse sine of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'asin';
};


CREATE FUNCTION
std::math::atan(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the inverse tangent of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'atan';
};


CREATE FUNCTION
std::math::atan2(y: std::float64, x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the inverse tangent of y/x of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'atan2';
};


CREATE FUNCTION
std::math::cos(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the cosine of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'cos';
};


CREATE FUNCTION
std::math::cot(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the cotangent of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'cot';
};


CREATE FUNCTION
std::math::sin(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sine of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sin';
};


CREATE FUNCTION
std::math::tan(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the tangent of the input value.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'tan';
};
