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


scalar type custom_str_t extending str {
    constraint regexp('[A-Z]+');
}

type Test {
    property p_bool -> bool;
    property p_str -> str;
    property p_datetime -> datetime;
    property p_local_datetime -> cal::local_datetime;
    property p_local_date -> cal::local_date;
    property p_local_time -> cal::local_time;
    property p_duration -> duration;
    property p_int16 -> int16;
    property p_int32 -> int32;
    property p_int64 -> int64;
    property p_float32 -> float32;
    property p_float64 -> float64;
    property p_bigint -> bigint;
    property p_decimal -> decimal;
}

type JSONTest {
    property j_bool -> json;
    property j_str -> json;
    property j_datetime -> json;
    property j_local_datetime -> json;
    property j_local_date -> json;
    property j_local_time -> json;
    property j_duration -> json;
    property j_int16 -> json;
    property j_int32 -> json;
    property j_int64 -> json;
    property j_float32 -> json;
    property j_float64 -> json;
    property j_bigint -> json;
    property j_decimal -> json;
}

type ScalarTest {
    property p_bool -> bool;
    property p_uuid -> uuid;
    property p_str -> str;
    property p_datetime -> datetime;
    property p_local_datetime -> cal::local_datetime;
    property p_local_date -> cal::local_date;
    property p_local_time -> cal::local_time;
    property p_duration -> duration;
    property p_int16 -> int16;
    property p_int32 -> int32;
    property p_int64 -> int64;
    property p_float32 -> float32;
    property p_float64 -> float64;
    property p_bigint -> bigint;
    property p_decimal -> decimal;
    property p_json -> json;
}
