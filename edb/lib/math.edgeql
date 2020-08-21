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

CREATE MODULE math;


CREATE FUNCTION
math::abs(x: std::anyreal) -> std::anyreal
{
    CREATE ANNOTATION std::description :=
        'Return the absolute value of the input *x*.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'abs';
};


CREATE FUNCTION
math::ceil(x: std::int64) -> std::int64
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT "x";';
};


CREATE FUNCTION
math::ceil(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT ceil("x");'
};


CREATE FUNCTION
math::ceil(x: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT "x";'
};


CREATE FUNCTION
math::ceil(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round up to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT ceil("x");'
};


CREATE FUNCTION
math::floor(x: std::int64) -> std::int64
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT "x";';
};


CREATE FUNCTION
math::floor(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT floor("x");';
};


CREATE FUNCTION
math::floor(x: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT "x";'
};


CREATE FUNCTION
math::floor(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round down to the nearest integer.';
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT floor("x");';
};


CREATE FUNCTION
math::ln(x: std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the natural logarithm of the input value.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'ln';
};


CREATE FUNCTION
math::ln(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the natural logarithm of the input value.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'ln';
};


CREATE FUNCTION
math::ln(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the natural logarithm of the input value.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'ln';
};


CREATE FUNCTION
math::lg(x: std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the base 10 logarithm of the input value.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'log';
};


CREATE FUNCTION
math::lg(x: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the base 10 logarithm of the input value.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'log';
};


CREATE FUNCTION
math::lg(x: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the base 10 logarithm of the input value.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'log';
};


CREATE FUNCTION
math::log(x: std::decimal, NAMED ONLY base: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the logarithm of the input value in the specified *base*.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT log("base", "x")
    $$;
};


# math::mean
# -----------
# The mean function returns an empty set if the input is empty set. On
# all other inputs it returns the mean for that input set.
CREATE FUNCTION
math::mean(vals: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the arithmetic mean of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'avg';
    SET error_on_null_result := 'invalid input to mean(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::mean(vals: SET OF std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the arithmetic mean of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'avg';
    # SQL 'avg' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to mean(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::mean(vals: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the arithmetic mean of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'avg';
    SET error_on_null_result := 'invalid input to mean(): not ' ++
                                'enough elements in input set';
};


# math::stddev
# ------------
CREATE FUNCTION
math::stddev(vals: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the sample standard deviation of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'stddev';
    SET error_on_null_result := 'invalid input to stddev(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::stddev(vals: SET OF std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample standard deviation of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'stddev';
    # SQL 'stddev' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to stddev(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::stddev(vals: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample standard deviation of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'stddev';
    SET error_on_null_result := 'invalid input to stddev(): not ' ++
                                'enough elements in input set';
};


# math::stddev_pop
# ----------------
CREATE FUNCTION
math::stddev_pop(vals: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the population standard deviation of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'stddev_pop';
    SET error_on_null_result := 'invalid input to stddev_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::stddev_pop(vals: SET OF std::int64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population standard deviation of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'stddev_pop';
    # SQL 'stddev_pop' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to stddev_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::stddev_pop(vals: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population standard deviation of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'stddev_pop';
    SET error_on_null_result := 'invalid input to stddev_pop(): not ' ++
                                'enough elements in input set';
};


# math::var
# --------------
CREATE FUNCTION
math::var(vals: SET OF std::decimal) -> OPTIONAL std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the sample variance of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'variance';
    SET error_on_null_result := 'invalid input to var(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::var(vals: SET OF std::int64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample variance of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'variance';
    # SQL 'var' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to var(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::var(vals: SET OF std::float64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sample variance of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'variance';
    SET error_on_null_result := 'invalid input to var(): not ' ++
                                'enough elements in input set';
};


# math::var_pop
# -------------
CREATE FUNCTION
math::var_pop(vals: SET OF std::decimal) -> OPTIONAL std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the population variance of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'var_pop';
    SET error_on_null_result := 'invalid input to var_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::var_pop(vals: SET OF std::int64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population variance of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'var_pop';
    # SQL 'var_pop' returns numeric on integer inputs.
    SET force_return_cast := true;
    SET error_on_null_result := 'invalid input to var_pop(): not ' ++
                                'enough elements in input set';
};


CREATE FUNCTION
math::var_pop(vals: SET OF std::float64) -> OPTIONAL std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the population variance of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'var_pop';
    SET error_on_null_result := 'invalid input to var_pop(): not ' ++
                                'enough elements in input set';
};
