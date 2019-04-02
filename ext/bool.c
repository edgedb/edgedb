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
#include "fmgr.h"
#include "utils/builtins.h"


/* A version of boolin() that only accepts variants of "true" and "false"
 * as valid text representation of std::bool.
 */
PG_FUNCTION_INFO_V1(edb_bool_in);

Datum
edb_bool_in(PG_FUNCTION_ARGS)
{
	text	   *txt = PG_GETARG_TEXT_PP(0);
	char	   *in_str;
	char	   *str;
	size_t		len;
	bool		result;
	bool		valid;

	in_str = text_to_cstring(txt);

	str = in_str;
	while (isspace((unsigned char) *str))
		str++;

	len = strlen(str);
	while (len > 0 && isspace((unsigned char) str[len - 1]))
		len--;

	result = false;
	valid = false;
	switch (*str)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(str, "true", len) == 0)
			{
				result = true;
				valid = true;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(str, "false", len) == 0)
			{
				result = false;
				valid = true;
			}
			break;
		default:
			break;
	}

	pfree(in_str);

	if (!valid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid syntax for bool: \"%s\"", in_str)));

	PG_RETURN_BOOL(result);
}
