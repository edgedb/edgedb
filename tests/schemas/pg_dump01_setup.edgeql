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


SET MODULE default;

INSERT A {
    p_bool := True,
    p_str := 'Hello',
    p_datetime := <datetime>'2018-05-07T20:01:22.306916+00:00',
    p_local_datetime := <cal::local_datetime>'2018-05-07T20:01:22.306916',
    p_local_date := <cal::local_date>'2018-05-07',
    p_local_time := <cal::local_time>'20:01:22.306916',
    p_duration := <duration>'20 hrs',
    p_relative_duration := <cal::relative_duration>'3 days 4 hrs',
    p_date_duration := <cal::date_duration>'2 months 5 days',
    p_int16 := 12345,
    p_int32 := 1234567890,
    p_int64 := 1234567890123,
    p_float32 := 2.5,
    p_float64 := 2.5,
    p_bigint := 123456789123456789123456789n,
    p_decimal := 123456789123456789123456789.123456789123456789123456789n,
    p_json := to_json('[{"a": null, "b": true}, 1, 2.5, "foo"]'),
    p_bytes := b'Hello',
};

INSERT B {
    arr_bool := [True, False],
    arr_str := ['Hello', 'world'],
    arr_datetime := [<datetime>'2018-05-07T20:01:22.306916+00:00'],
    arr_local_datetime := [<cal::local_datetime>'2018-05-07T20:01:22.306916'],
    arr_local_date := [<cal::local_date>'2018-05-07'],
    arr_local_time := [<cal::local_time>'20:01:22.306916'],
    arr_duration := [<duration>'20 hrs'],
    arr_relative_duration := [<cal::relative_duration>'3 days 4 hrs'],
    arr_date_duration := [<cal::date_duration>'2 months 5 days'],
    arr_int16 := [12345],
    arr_int32 := [1234567890],
    arr_int64 := [1234567890123],
    arr_float32 := [2.5],
    arr_float64 := [2.5],
    arr_bigint := [123456789123456789123456789n],
    arr_decimal := [123456789123456789123456789.123456789123456789123456789n],
    arr_json := [to_json('[{"a": null, "b": true}, 1, 2.5, "foo"]')],
    arr_bytes := [b'Hello', b'world'],
};

INSERT C {
    tup0 := (
      True,
      'Hello',
      <datetime>'2018-05-07T20:01:22.306916+00:00',
    ),
    tup1 := (
      <cal::local_datetime>'2018-05-07T20:01:22.306916',
      <cal::local_date>'2018-05-07',
      <cal::local_time>'20:01:22.306916',
      <duration>'20 hrs',
    ),
    tup2 := (
      <cal::relative_duration>'3 days 4 hrs',
      <cal::date_duration>'2 months 5 days',
      to_json('[{"a": null, "b": true}, 1, 2.5, "foo"]'),
      b'Hello',
    ),
    tup3 := (
      12345,
      1234567890,
      1234567890123,
      2.5,
      2.5,
      123456789123456789123456789n,
      123456789123456789123456789.123456789123456789123456789n,
    ),

    nested0 := [
        ([([0, 1],), ([2, 3, 4],)],),
        ([([5, 6],), (<array<int64>>[],)],),
    ],
    nested1 := (
        <Positive>2,
        'some string',
        (
            (-1.2, False),
            (
                <cal::local_date>'2023-05-17',
                <cal::local_time>'21:43:56',
            ),
        ),
    ),

    r_int32 := range(<int32>1, <int32>20),
    r_int64 := range(2, 123456789012),
    r_float32 := range(<float32>1.1, <float32>2.2),
    r_float64 := range(0.1, 2.3),
    r_decimal := range(1.2n, 3.4n),
    r_datetime := range(<datetime>'2022-01-31T11:22:33Z',
                        <datetime>'2024-03-31T17:28:04Z'),
    r_local_datetime := range(<cal::local_datetime>'2022-02-15T11:22:33',
                              <cal::local_datetime>'2024-04-25T17:28:04'),
    r_local_date := range(<cal::local_date>'2022-03-17',
                          <cal::local_date>'2024-05-31'),
    pos := <Positive>1234,
};
