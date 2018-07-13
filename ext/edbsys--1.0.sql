-- This source file is part of the EdgeDB open source project.
--
-- Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION recordext" to load this file. \quit

--
-- "Bless" the passed record.
--
CREATE FUNCTION bless_record(record)
RETURNS record
AS '$libdir/recordext'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;


--
-- Return the given attribute value from a row value.
--
CREATE FUNCTION row_getattr_by_num(record, integer, anyelement)
RETURNS anyelement
AS '$libdir/recordext'
LANGUAGE C CALLED ON NULL INPUT IMMUTABLE PARALLEL SAFE;


--
-- Convert the given record into a jsonb array.
--
CREATE FUNCTION row_to_jsonb_array(record)
RETURNS jsonb
AS '$libdir/recordext'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;
