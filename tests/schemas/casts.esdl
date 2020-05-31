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
    optional property p_bool -> bool;
    optional property p_str -> str;
    optional property p_datetime -> datetime;
    optional property p_local_datetime -> cal::local_datetime;
    optional property p_local_date -> cal::local_date;
    optional property p_local_time -> cal::local_time;
    optional property p_duration -> duration;
    optional property p_int16 -> int16;
    optional property p_int32 -> int32;
    optional property p_int64 -> int64;
    optional property p_float32 -> float32;
    optional property p_float64 -> float64;
    optional property p_bigint -> bigint;
    optional property p_decimal -> decimal;
}

type JSONTest {
    optional property j_bool -> json;
    optional property j_str -> json;
    optional property j_datetime -> json;
    optional property j_local_datetime -> json;
    optional property j_local_date -> json;
    optional property j_local_time -> json;
    optional property j_duration -> json;
    optional property j_int16 -> json;
    optional property j_int32 -> json;
    optional property j_int64 -> json;
    optional property j_float32 -> json;
    optional property j_float64 -> json;
    optional property j_bigint -> json;
    optional property j_decimal -> json;
}

type ScalarTest {
    optional property p_bool -> bool;
    optional property p_uuid -> uuid;
    optional property p_str -> str;
    optional property p_datetime -> datetime;
    optional property p_local_datetime -> cal::local_datetime;
    optional property p_local_date -> cal::local_date;
    optional property p_local_time -> cal::local_time;
    optional property p_duration -> duration;
    optional property p_int16 -> int16;
    optional property p_int32 -> int32;
    optional property p_int64 -> int64;
    optional property p_float32 -> float32;
    optional property p_float64 -> float64;
    optional property p_bigint -> bigint;
    optional property p_decimal -> decimal;
    optional property p_json -> json;
}
