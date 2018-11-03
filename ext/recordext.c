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

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


static const char *_get_type_name(Oid typeoid);


PG_FUNCTION_INFO_V1(bless_record);

Datum
bless_record(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec;
	Oid				tup_type;
	int32			tup_typmod;
	TupleDesc		tup_desc;

	rec = PG_GETARG_HEAPTUPLEHEADER(0);
	tup_type = HeapTupleHeaderGetTypeId(rec);
	tup_typmod = HeapTupleHeaderGetTypMod(rec);
	tup_desc = lookup_rowtype_tupdesc(tup_type, tup_typmod);

	BlessTupleDesc(tup_desc);
	ReleaseTupleDesc(tup_desc);

	PG_RETURN_HEAPTUPLEHEADER(rec);
}


/*
 * SQL function row_getattr_by_num(record, attnum, any) -> any
 *
 * This is essentially equivalent to the GetAttributeByNum()
 * function.  The function is polymorphic, the caller must
 * pass the type of the returned attribute value in the
 * third argument as NULL::<type>.
 */
PG_FUNCTION_INFO_V1(row_getattr_by_num);

Datum
row_getattr_by_num(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	int				attnum = PG_GETARG_INT32(1);
	Oid				val_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	HeapTupleData	tuple;
	Oid				tup_type;
	int32			tup_typmod;
	TupleDesc		tup_desc;
	int				i, j;
	Datum			val;
	Oid				att_type;
	bool			isnull;
	Form_pg_attribute att;

	if (!AttributeNumberIsValid(attnum))
		elog(ERROR, "invalid attribute number %d", attnum);

	tup_type = HeapTupleHeaderGetTypeId(rec);
	tup_typmod = HeapTupleHeaderGetTypMod(rec);
	tup_desc = lookup_rowtype_tupdesc(tup_type, tup_typmod);

	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	for (i = 0, j = 0; i < tup_desc->natts; i++)
	{
		att = TupleDescAttr(tup_desc, i);

		if (att->attisdropped)
			continue;

		j += 1;

		if (j == attnum)
		{
			val = heap_getattr(&tuple, i + 1, tup_desc, &isnull);
			break;
		}
	}

	att_type = att->atttypid;
	if (att_type == UNKNOWNOID)
	{
		/* Uncasted string literal come in as a cstring pointer,
		 * and we must cast it into text before returning.
		 */
		 val = CStringGetTextDatum(DatumGetCString(val));
		 att_type = TEXTOID;
	}

	ReleaseTupleDesc(tup_desc);

	if (att_type != val_type)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected tuple attribute type \"%s\", got \"%s\"",
						_get_type_name(val_type),
						_get_type_name(att->atttypid))));

	PG_RETURN_DATUM(val);
}


/*
 * SQL function row_to_jsonb_array(record) -> jsonb
 *
 * Built-in jsonb functions serialize anonymous records to
 * JSON objects of the form {"f1": <attr1>, ... "fN": <attrN>},
 * which is useless for tuple serialization semantics in EdgeDB.
 *
 * This function produces a JSON array from an arbitrary record
 * Datum by forwarding the record's attributes to
 * jsonb_build_array().
 */
PG_FUNCTION_INFO_V1(row_to_jsonb_array);

Datum
row_to_jsonb_array(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec;
	HeapTupleData	tmptup;
	List			*jbba_name = NIL;
	List			*jbba_args = NIL;
	Oid				jbba_argtypes[1];	/* dummy */
	Oid				jbba_oid;
	FmgrInfo		jbba_finfo;
	FunctionCallInfoData jbba_fcinfo;
	Oid				tup_type;
	int32			tup_typmod;
	TupleDesc		tup_desc;
	int				i, argno = 0;

	rec = PG_GETARG_HEAPTUPLEHEADER(0);
	tup_type = HeapTupleHeaderGetTypeId(rec);
	tup_typmod = HeapTupleHeaderGetTypMod(rec);
	tup_desc = lookup_rowtype_tupdesc(tup_type, tup_typmod);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(rec);
	tmptup.t_data = rec;

	jbba_argtypes[0] = ANYOID;
	jbba_name = list_make1(makeString("jsonb_build_array"));
	jbba_oid = LookupFuncName(jbba_name, 1, jbba_argtypes, false);

	fmgr_info(jbba_oid, &jbba_finfo);

	InitFunctionCallInfoData(jbba_fcinfo, &jbba_finfo,
							 tup_desc->natts,
							 InvalidOid, NULL, NULL);

	for (i = 0; i < tup_desc->natts; i++)
	{
		Datum		val;
		Const		*argnode;
		bool		isnull;
		Form_pg_attribute att = TupleDescAttr(tup_desc, i);

		if (att->attisdropped)
			continue;

		val = heap_getattr(&tmptup, i + 1, tup_desc, &isnull);

		jbba_fcinfo.argnull[argno] = isnull;
		jbba_fcinfo.arg[argno] = val;

		if (isnull)
			argnode = makeNullConst(
				att->atttypid, att->atttypmod, att->attcollation);
		else
			argnode = makeConst(
				att->atttypid, att->atttypmod, att->attcollation,
				att->attlen, val, false, att->attbyval);

		jbba_args = lappend(jbba_args, argnode);

		argno++;
	}

	ReleaseTupleDesc(tup_desc);

	/* jsonb_build_array() is polymorphic, so we need the fake
	 * the function call expression in order for it to be able
	 * to infer the argument types.
	 */
	fmgr_info_set_expr(
		(Node *)makeFuncExpr(
			jbba_oid, get_func_rettype(jbba_oid), jbba_args,
			InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL),
		&jbba_finfo);

	PG_RETURN_DATUM(FunctionCallInvoke(&jbba_fcinfo));
}


static const char *
_get_type_name(Oid typeoid)
{
	HeapTuple		type_tuple;
	Form_pg_type 	type_struct;
	const char 		*result;

	type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeoid));
	if (!HeapTupleIsValid(type_tuple))
		elog(ERROR, "cache lookup failed for type %u", typeoid);

	type_struct = (Form_pg_type) GETSTRUCT(type_tuple);
	result = pstrdup(NameStr(type_struct->typname));

	ReleaseSysCache(type_tuple);

	return result;
}
