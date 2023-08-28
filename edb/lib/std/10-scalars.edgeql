#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


CREATE PSEUDO TYPE `anytype`;

CREATE PSEUDO TYPE `anytuple`;

CREATE PSEUDO TYPE `anyobject`;

CREATE ABSTRACT SCALAR TYPE std::anyscalar;

CREATE ABSTRACT SCALAR TYPE std::anypoint EXTENDING std::anyscalar;

CREATE ABSTRACT SCALAR TYPE std::anydiscrete EXTENDING std::anypoint;

CREATE ABSTRACT SCALAR TYPE std::anycontiguous EXTENDING std::anypoint;

CREATE SCALAR TYPE std::bool EXTENDING std::anyscalar;

CREATE SCALAR TYPE std::bytes EXTENDING std::anyscalar;

CREATE SCALAR TYPE std::uuid EXTENDING std::anyscalar;

CREATE SCALAR TYPE std::str EXTENDING std::anyscalar;

CREATE SCALAR TYPE std::json EXTENDING std::anyscalar;

CREATE SCALAR TYPE std::datetime EXTENDING std::anycontiguous;

CREATE SCALAR TYPE std::duration EXTENDING std::anycontiguous;

CREATE ABSTRACT SCALAR TYPE std::anyreal EXTENDING std::anyscalar;

CREATE ABSTRACT SCALAR TYPE std::anyint EXTENDING std::anyreal;

CREATE SCALAR TYPE std::int16 EXTENDING std::anyint;

CREATE SCALAR TYPE std::int32 EXTENDING std::anyint, std::anydiscrete;

CREATE SCALAR TYPE std::int64 EXTENDING std::anyint, std::anydiscrete;

CREATE ABSTRACT SCALAR TYPE std::anyfloat
    EXTENDING std::anyreal, std::anycontiguous;

CREATE SCALAR TYPE std::float32 EXTENDING std::anyfloat;

CREATE SCALAR TYPE std::float64 EXTENDING std::anyfloat;

CREATE ABSTRACT SCALAR TYPE std::anynumeric EXTENDING std::anyreal;

CREATE SCALAR TYPE std::decimal EXTENDING std::anynumeric, std::anycontiguous;

CREATE SCALAR TYPE std::bigint EXTENDING std::anynumeric, std::anyint;

CREATE ABSTRACT SCALAR TYPE std::sequence EXTENDING std::int64;

CREATE ABSTRACT SCALAR TYPE std::anyenum EXTENDING std::anyscalar;
