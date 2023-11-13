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


insert Setting {
    name := 'template',
    value := 'blue'
};

insert Setting {
    name := 'perks',
    value := 'full'
};

insert UserGroup {
    name := 'basic'
};

insert UserGroup {
    name := 'upgraded',
    settings := (select Setting)
};

insert UserGroup {
    name := 'unused',
    settings := (
        insert Setting {
            name := 'template',
            value := 'none'
        }
    )
};

insert User {
    name := 'John',
    age := 25,
    active := True,
    score := 3.14,
    groups := (select UserGroup filter UserGroup.name = 'basic')
};

insert User {
    name := 'Jane',
    age := 25,
    active := True,
    score := 1.23,
    groups := (select UserGroup filter UserGroup.name = 'upgraded')
};

insert User {
    name := 'Alice',
    age := 27,
    active := True,
    score := 5.0,
    profile := (insert Profile {
        name := 'Alice profile',
        value := 'special',
        tags := ['1st', '2nd'],
    }),
    favorites := {Setting, UserGroup}
};

insert Person {
    name := 'Bob',
    age := 21,
    active := True,
    score := 4.2,
    profile := (insert Profile {
        name := 'Bob profile',
        value := 'special',
    }),
    favorites := {UserGroup, User}
};

WITH MODULE other
insert Foo {
    `select` := 'a',
    color := 'RED',
};

WITH MODULE other
insert Foo {
    `select` := 'b',
    after := 'w',
    color := 'GREEN',
};

WITH MODULE other
insert Foo {
    after := 'q',
    color := 'BLUE',
};

insert ScalarTest {
    p_bool := True,
    p_str := 'Hello world',
    p_datetime := <datetime>'2018-05-07T20:01:22.306916+00:00',
    p_local_datetime := <cal::local_datetime>'2018-05-07T20:01:22.306916',
    p_local_date := <cal::local_date>'2018-05-07',
    p_local_time := <cal::local_time>'20:01:22.306916',
    p_duration := <duration>'20 hrs',
    p_int16 := 12345,
    p_int32 := 1234567890,
    p_int64 := 1234567890123,
    p_bigint := 123456789123456789123456789n,
    p_float32 := 2.5,
    p_float64 := 2.5,
    p_decimal :=
        123456789123456789123456789.123456789123456789123456789n,
    p_json := to_json('{"foo": [1, null, "bar"]}'),
    p_bytes := b'Hello World',

    p_posint := 42,
    p_array_str := ['hello', 'world'],
    p_array_json := [<json>'hello', <json>'world'],
    p_array_bytes := [b'hello', b'world'],

    p_tuple := (123, 'test'),
    p_array_tuple := [('hello', true), ('world', false)],

    p_short_str := 'hello',
};

# Inheritance tests
insert Bar {
    q := 'bar'
};


insert Bar2 {
    q := 'bar2',
    w := 'special'
};


insert Rab {
    blah := (select Bar limit 1)
};


insert Rab2 {
    blah := (select Bar2 limit 1)
};


insert LinkedList {
    name := '4th',
};

insert LinkedList {
    name := '3rd',
    next := (
        select DETACHED LinkedList
        filter .name = '4th'
        limit 1
    )
};

insert LinkedList {
    name := '2nd',
    next := (
        select DETACHED LinkedList
        filter .name = '3rd'
        limit 1
    )
};

insert LinkedList {
    name := '1st',
    next := (
        select DETACHED LinkedList
        filter .name = '2nd'
        limit 1
    )
};

insert Combo {
    name := 'combo 0',
};

insert Combo {
    name := 'combo 1',
    data := assert_single((
        select Setting filter .name = 'template' and .value = 'blue'
    )),
};

insert Combo {
    name := 'combo 2',
    data := assert_single((select Profile filter .name = 'Alice profile')),
};

insert ErrorTest {
    text := ')error(',
    val := 0,
};

insert RangeTest {
    name := 'test01',
    rval := range(-1.3, 1.2),
    mval := multirange([
        range(<float64>{}, -10.0),
        range(-1.3, 1.2),
        range(10.0),
    ]),
    rdate := range(
        <cal::local_date>'2018-01-23',
        <cal::local_date>'2023-07-25',
    ),
    mdate := multirange([
        range(
            <cal::local_date>'2018-01-23',
            <cal::local_date>'2023-07-25',
        ),
        range(
            <cal::local_date>'2025-11-22',
        ),
    ]),
};

insert RangeTest {
    name := 'missing boundaries',
    # empty
    rval := range(<float64>{}, empty := true),
    # unbounded = everything
    mval := multirange([range(<float64>{})]),
    # empty
    rdate := range(<cal::local_date>{}, empty := true),
    # unbounded = everything
    mdate := multirange([range(<cal::local_date>{})]),
};

insert Fixed;

insert NotEditable {
    once := 'init',
};

insert other::deep::NestedMod {
    val := 'in nested module'
};