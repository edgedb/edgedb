#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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


CREATE MODULE std::pg;

CREATE ABSTRACT INDEX std::pg::hash {
    CREATE ANNOTATION std::description :=
        'Index based on a 32-bit hash derived from the indexed value.';
    SET code := 'hash ((__col__))';
};

create index match for anytype using std::pg::hash;

CREATE ABSTRACT INDEX std::pg::btree {
    CREATE ANNOTATION std::description :=
        'B-tree index can be used to retrieve data in sorted order.';
    SET code := 'btree ((__col__) NULLS FIRST)';
};

create index match for anytype using std::pg::btree;

CREATE ABSTRACT INDEX std::pg::gin {
    CREATE ANNOTATION std::description :=
        'GIN is an "inverted index" appropriate for data values that \
        contain multiple elements, such as arrays and JSON.';
    SET code := 'gin ((__col__))';
};

create index match for array<anytype> using std::pg::gin;
create index match for std::json using std::pg::gin;

CREATE ABSTRACT INDEX std::pg::gist {
    CREATE ANNOTATION std::description :=
        'GIST index can be used to optimize searches involving ranges.';
    SET code := 'gist ((__col__))';
};

create index match for array<anytype> using std::pg::gist;
create index match for range<std::anypoint> using std::pg::gist;
create index match for multirange<std::anypoint> using std::pg::gist;

CREATE ABSTRACT INDEX std::pg::spgist {
    CREATE ANNOTATION std::description :=
        'SP-GIST index can be used to optimize searches involving ranges \
        and strings.';
    SET code := 'spgist ((__col__))';
};

create index match for range<std::anypoint> using std::pg::spgist;
create index match for std::str using std::pg::spgist;

CREATE ABSTRACT INDEX std::pg::brin {
    CREATE ANNOTATION std::description :=
        'BRIN (Block Range INdex) index works with summaries about the values \
        stored in consecutive physical block ranges in the database.';
    SET code := 'brin ((__col__))';
};

create index match for range<std::anypoint> using std::pg::brin;
create index match for std::anyreal using std::pg::brin;
create index match for std::bytes using std::pg::brin;
create index match for std::str using std::pg::brin;
create index match for std::uuid using std::pg::brin;
create index match for std::datetime using std::pg::brin;
create index match for std::duration using std::pg::brin;
create index match for std::cal::local_datetime using std::pg::brin;
create index match for std::cal::local_date using std::pg::brin;
create index match for std::cal::local_time using std::pg::brin;
create index match for std::cal::relative_duration using std::pg::brin;
create index match for std::cal::date_duration using std::pg::brin;

create scalar type std::pg::json extending std::anyscalar;
create scalar type std::pg::timestamptz extending std::anycontiguous;
create scalar type std::pg::timestamp extending std::anycontiguous;
create scalar type std::pg::date extending std::anydiscrete;
create scalar type std::pg::interval extending std::anycontiguous;
