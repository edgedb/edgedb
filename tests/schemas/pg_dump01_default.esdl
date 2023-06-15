#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


type A {
    annotation title := 'A';

    property p_bool -> bool {
        annotation title := 'single bool';
    }
    property p_str -> str;
    property p_datetime -> datetime;
    property p_local_datetime -> cal::local_datetime;
    property p_local_date -> cal::local_date;
    property p_local_time -> cal::local_time;
    property p_duration -> duration;
    property p_relative_duration -> cal::relative_duration;
    property p_date_duration -> cal::date_duration;
    property p_int16 -> int16;
    property p_int32 -> int32;
    property p_int64 -> int64;
    property p_float32 -> float32;
    property p_float64 -> float64;
    property p_bigint -> bigint;
    property p_decimal -> decimal;
    property p_json -> json;
    property p_bytes -> bytes;
}

type B {
    annotation title := 'B';

    property arr_bool -> array<bool>;
    property arr_str -> array<str>;
    property arr_datetime -> array<datetime>;
    property arr_local_datetime -> array<cal::local_datetime>;
    property arr_local_date -> array<cal::local_date>;
    property arr_local_time -> array<cal::local_time>;
    property arr_duration -> array<duration>;
    property arr_relative_duration -> array<cal::relative_duration>;
    property arr_date_duration -> array<cal::date_duration>;
    property arr_int16 -> array<int16>;
    property arr_int32 -> array<int32>;
    property arr_int64 -> array<int64>;
    property arr_float32 -> array<float32>;
    property arr_float64 -> array<float64>;
    property arr_bigint -> array<bigint>;
    property arr_decimal -> array<decimal>;
    property arr_json -> array<json>;
    property arr_bytes -> array<bytes>;
}

type C {
    annotation title := 'C';

    property tup0 -> tuple<bool, str, datetime>;
    property tup1 -> tuple<
                        cal::local_datetime,
                        cal::local_date,
                        cal::local_time,
                        duration
                     >;
    property tup2 -> tuple<
                        cal::relative_duration,
                        cal::date_duration,
                        json,
                        bytes
                     >;
    property tup3 -> tuple<
                        int16,
                        int32,
                        int64,
                        float32,
                        float64,
                        bigint,
                        decimal
                     >;

    property nested0 -> array<tuple<array<tuple<array<int64>>>>>;
    property nested1 -> tuple<
                            Positive,
                            str,
                            tuple<
                                tuple<
                                    float64,
                                    bool,
                                >,
                                tuple<
                                    cal::local_date,
                                    cal::local_time,
                                >,
                            >,
                        >;

    property r_int32 -> range<std::int32>;
    property r_int64 -> range<std::int64>;
    property r_float32 -> range<std::float32>;
    property r_float64 -> range<std::float64>;
    property r_decimal -> range<std::decimal>;
    property r_datetime -> range<std::datetime>;
    property r_local_datetime -> range<cal::local_datetime>;
    property r_local_date -> range<cal::local_date>;

    property pos -> Positive;
}

scalar type Positive extending int64 {
  constraint min_value(1);
};
