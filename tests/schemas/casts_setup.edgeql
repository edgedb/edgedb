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


WITH MODULE test
INSERT Test {
    p_bool := True,
    p_str := 'Hello',
    p_datetime := <datetime>'2018-05-07T20:01:22.306916+00:00',
    p_local_datetime := <cal::local_datetime>'2018-05-07T20:01:22.306916',
    p_local_date := <cal::local_date>'2018-05-07',
    p_local_time := <cal::local_time>'20:01:22.306916',
    p_duration := <duration>'20 hrs',
    p_int16 := 12345,
    p_int32 := 1234567890,
    p_int64 := 1234567890123,
    p_float32 := 2.5,
    p_float64 := 2.5,
    p_bigint := 123456789123456789123456789n,
    p_decimal := 123456789123456789123456789.123456789123456789123456789n,
};


WITH MODULE test
INSERT JSONTest {
    j_bool := <json>True,
    j_str := <json>'Hello',
    j_datetime := <json><datetime>'2018-05-07T20:01:22.306916+00:00',
    j_local_datetime := <json><cal::local_datetime>'2018-05-07T20:01:22.306916',
    j_local_date := <json><cal::local_date>'2018-05-07',
    j_local_time := <json><cal::local_time>'20:01:22.306916',
    j_duration := <json><duration>'20 hrs',
    j_int16 := <json>12345,
    j_int32 := <json>1234567890,
    j_int64 := <json>1234567890123,
    j_float32 := <json>2.5,
    j_float64 := <json>2.5,
    j_bigint := <json>123456789123456789123456789n,
    j_decimal := <json>123456789123456789123456789.123456789123456789123456789n
};
