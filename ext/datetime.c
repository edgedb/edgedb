/*
 * Portions Copyright (c) 2019 MagicStack Inc. and the EdgeDB authors.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */


#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/formatting.h"
#include "utils/nabstime.h"


static int	tm2time(struct pg_tm *tm, fsec_t fsec, TimeADT *result);


/* A version of interval_out() which spells months as "month", not "mon". */
PG_FUNCTION_INFO_V1(edb_interval_out);

Datum
edb_interval_out(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char		buf[MAXDATELEN + 1];

	if (interval2tm(*span, tm, &fsec) != 0)
		elog(ERROR, "could not convert interval to tm");

	EncodeInterval(tm, fsec, INTSTYLE_EDGEDB, buf);

	result = pstrdup(buf);
	PG_RETURN_TEXT_P(cstring_to_text(result));
}


/* A version of to_timestamp() which errors out if the provided
 * datetime format contains a timezone.
 */
PG_FUNCTION_INFO_V1(edb_to_timestamp);

Datum
edb_to_timestamp(PG_FUNCTION_ARGS)
{
	text	   *date_txt = PG_GETARG_TEXT_PP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	Timestamp	result;
	struct pg_tm tm;
	fsec_t		fsec;

	EdgeDBToTimestamp(date_txt, fmt, &tm, &fsec, EDGEDB_TZ_PROHIBITED);

	if (tm.tm_zone)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("cannot convert to timestamp: "
				 	    "there is an explicit timezone")));

	if (tm2timestamp(&tm, fsec, NULL, &result) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}


/* A version of to_timestamptz() which errors out if the provided
 * datetime format contains *no* timezone.
 */
PG_FUNCTION_INFO_V1(edb_to_timestamptz);

Datum
edb_to_timestamptz(PG_FUNCTION_ARGS)
{
	text	   *date_txt = PG_GETARG_TEXT_PP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	Timestamp	result;
	int			tz;
	struct pg_tm tm;
	fsec_t		fsec;
	int			dterr;

	EdgeDBToTimestamp(date_txt, fmt, &tm, &fsec, EDGEDB_TZ_REQUIRED);

	if (!tm.tm_zone)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("cannot convert to timestamptz: "
				 	    "there is no explicit timezone")));

	dterr = DecodeTimezone((char *) tm.tm_zone, &tz);
	if (dterr)
		DateTimeParseError(dterr, text_to_cstring(date_txt), "timestamptz");

	if (tm2timestamp(&tm, fsec, &tz, &result) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}


/* tm2time()
 * Convert a tm structure to a time data type.
 */
static int
tm2time(struct pg_tm *tm, fsec_t fsec, TimeADT *result)
{
	*result = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) +
			tm->tm_sec) * USECS_PER_SEC) + fsec;
	return 0;
}
