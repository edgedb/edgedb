/*
 * This source file is part of the EdgeDB open source project.
 *
 * Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "postgres.h"

#include <float.h>
#include <math.h>

#include "common/int.h"
#include "funcapi.h"


static void
int_floor_divmod(int64 n, int64 d, int64 *p_q, int64 *p_r)
{
	int64 q, r;

	/* Integer overflow may happen if n == INT_MIN and d == -1,
	 * but we leave the check for that to the caller, since
	 * the value of the remainder in that case is defined to be 0.
	 * Similarly, the caller must check for zero division as well.
	 */

	q = n / d;

	/* C99 mandates truncating division, so the following will
	 * not overflow.
	 */
	r = n - q * d;

    /* If the the remainder is non-zero, and we had a negative numerator
	 * and a positive denominator, must compensate the C99 behavior of
	 * truncation towards zero (i.e. ceiling division).
     */
    if (r != 0 && ((d ^ r) < 0)) {
        r += d;
		q -= 1;
    }

	*p_q = q;
	*p_r = r;

    return;
}


static void
float4_floor_divmod(float4 n, float4 d, float4 *p_q, float4 *p_r)
{
	float4 approx_q, q, r;

	r = fmodf(n, d);
	approx_q = (n - r) / d;

	if (r)
	{
		if ((d < 0) != (r < 0))
		{
			/* C fmod() truncates toward zero, we want truncation
			 * toward negative infinity.
			 */
			r += d;
			approx_q -= 1.0;
		}
	}
	else
	{
		/* The remainder is zero, but may be -0, or +0, and this
		 * is platform-dependent.  For consistency, we want the
		 * sign of the remainder to always match that of the
		 * divisor.  We also avoid doing r = +0.0 or r = -0.0
		 * to make sure the compiler has no opportunity to
		 * (incorrectly) optimize this away.
		 */
		r *= r;
		if (d < 0)
			r = -r;
	}

	if (approx_q)
	{
		/* Make sure the quotient is an integral value. */
		q = floorf(approx_q);
		if (approx_q - q > 0.5)
			q += 1.0;
	}
	else
	{
		/* The quotient is zero, but may be -0, or +0, fix it to
		 * have the correct sign.
		 */
		q = approx_q * approx_q;
		if ((n < 0) != (d < 0))
			q = -q;
	}

	*p_q = q;
	*p_r = r;
}


static void
float8_floor_divmod(float8 n, float8 d, float8 *p_q, float8 *p_r)
{
	float8 approx_q, q, r;

	r = fmod(n, d); // -2
	approx_q = (n - r) / d; // 2

	if (r)
	{
		if ((d < 0) != (r < 0))
		{
			/* C fmod() truncates toward zero, we want truncation
			 * toward negative infinity.
			 */
			r += d;
			approx_q -= 1.0;
		}
	}
	else
	{
		/* The remainder is zero, but may be -0, or +0, and this
		 * is platform-dependent.  For consistency, we want the
		 * sign of the remainder to always match that of the
		 * divisor.  We also avoid doing r = +0.0 or r = -0.0
		 * to make sure the compiler has no opportunity to
		 * (incorrectly) optimize this away.
		 */
		r *= r;
		if (d < 0)
			r = -r;
	}

	if (approx_q)
	{
		/* Make sure the quotient is an integral value. */
		q = floor(approx_q);
		if (approx_q - q > 0.5)
			q += 1.0;
	}
	else
	{
		/* The quotient is zero, but may be -0, or +0, fix it to
		 * have the correct sign.
		 */
		q = approx_q * approx_q;
		if ((n < 0) != (d < 0))
			q = -q;
	}

	*p_q = q;
	*p_r = r;
}


/* A version of int2div() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_int2floordiv);

Datum
edb_int2floordiv(PG_FUNCTION_ARGS)
{
	int16		n = PG_GETARG_INT16(0);
	int16		d = PG_GETARG_INT16(1);
	int64		q;
	int64		r;

	if (unlikely(d == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	/* Same logic as in core int4div() */
	if (d == -1)
	{
		if (unlikely(n == PG_INT16_MIN))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("bigint out of range")));

		PG_RETURN_INT16(-n);
	}

	int_floor_divmod((int64)n, (int64)d, &q, &r);

	PG_RETURN_INT32((int16)q);
}


/* A version of int2mod() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_int2floormod);

Datum
edb_int2floormod(PG_FUNCTION_ARGS)
{
	int16		n = PG_GETARG_INT16(0);
	int16		d = PG_GETARG_INT16(1);
	int64		q;
	int64		r;

	if (unlikely(d == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	/* Same logic as in core int2mod(), guard for possible overflow
	 * in the case of INT16_MIN % -1.
	 */
	if (d == -1)
		PG_RETURN_INT16(0);

	int_floor_divmod((int64)n, (int64)d, &q, &r);

	PG_RETURN_INT16((int16)r);
}


/* A version of int4div() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_int4floordiv);

Datum
edb_int4floordiv(PG_FUNCTION_ARGS)
{
	int32		n = PG_GETARG_INT32(0);
	int32		d = PG_GETARG_INT32(1);
	int64		q;
	int64		r;

	if (unlikely(d == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	/* Same logic as in core int4div() */
	if (d == -1)
	{
		if (unlikely(n == PG_INT32_MIN))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("bigint out of range")));

		PG_RETURN_INT32(-n);
	}

	int_floor_divmod((int64)n, (int64)d, &q, &r);

	PG_RETURN_INT32((int32)q);
}


/* A version of int4mod() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_int4floormod);

Datum
edb_int4floormod(PG_FUNCTION_ARGS)
{
	int32		n = PG_GETARG_INT32(0);
	int32		d = PG_GETARG_INT32(1);
	int64		q;
	int64		r;

	if (unlikely(d == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	/* Same logic as in core int4mod(), guard for possible overflow
	 * in the case of INT32_MIN % -1.
	 */
	if (d == -1)
		PG_RETURN_INT32(0);

	int_floor_divmod((int64)n, (int64)d, &q, &r);

	PG_RETURN_INT32((int32)r);
}


/* A version of int8div() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_int8floordiv);

Datum
edb_int8floordiv(PG_FUNCTION_ARGS)
{
	int64		n = PG_GETARG_INT64(0);
	int64		d = PG_GETARG_INT64(1);
	int64		q;
	int64		r;

	if (unlikely(d == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	/* Same logic as in core int8div() */
	if (d == -1)
	{
		if (unlikely(n == PG_INT64_MIN))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("bigint out of range")));

		PG_RETURN_INT64(-n);
	}

	int_floor_divmod(n, d, &q, &r);

	PG_RETURN_INT64(q);
}


/* A version of int8mod() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_int8floormod);

Datum
edb_int8floormod(PG_FUNCTION_ARGS)
{
	int64		n = PG_GETARG_INT64(0);
	int64		d = PG_GETARG_INT64(1);
	int64		q;
	int64		r;

	if (unlikely(d == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	/* Same logic as in core int8mod(), guard for possible overflow
	 * in the case of INT64_MIN % -1.
	 */
	if (d == -1)
		PG_RETURN_INT64(0);

	int_floor_divmod(n, d, &q, &r);

	PG_RETURN_INT64(r);
}


/* A version of float4div() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_float4floordiv);

Datum
edb_float4floordiv(PG_FUNCTION_ARGS)
{
	float4		n = PG_GETARG_FLOAT4(0);
	float4		d = PG_GETARG_FLOAT4(1);
	float4		q;
	float4		r;

	if (unlikely(d == 0.0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	float4_floor_divmod(n, d, &q, &r);

	PG_RETURN_FLOAT4(q);
}


/* Floating-point division remainder which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_float4floormod);

Datum
edb_float4floormod(PG_FUNCTION_ARGS)
{
	float4		n = PG_GETARG_FLOAT4(0);
	float4		d = PG_GETARG_FLOAT4(1);
	float4		q;
	float4		r;

	if (unlikely(d == 0.0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	float4_floor_divmod(n, d, &q, &r);

	PG_RETURN_FLOAT4(r);
}


/* A version of float8div() which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_float8floordiv);

Datum
edb_float8floordiv(PG_FUNCTION_ARGS)
{
	float8		n = PG_GETARG_FLOAT8(0);
	float8		d = PG_GETARG_FLOAT8(1);
	float8		q;
	float8		r;

	if (unlikely(d == 0.0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	float8_floor_divmod(n, d, &q, &r);

	PG_RETURN_FLOAT8(q);
}


/* Floating-point division remainder which always preforms truncation towards
 * negative infinity, i.e. floor division.
 */
PG_FUNCTION_INFO_V1(edb_float8floormod);

Datum
edb_float8floormod(PG_FUNCTION_ARGS)
{
	float8		n = PG_GETARG_FLOAT8(0);
	float8		d = PG_GETARG_FLOAT8(1);
	float8		q;
	float8		r;

	if (unlikely(d == 0.0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
					errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	float8_floor_divmod(n, d, &q, &r);

	PG_RETURN_FLOAT8(r);
}
