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
#include "funcapi.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

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
