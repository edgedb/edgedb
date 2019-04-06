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


# FIXME: the computable in the view doesn't work without an explicit
# abstract link
CREATE ABSTRACT LINK default::of_group;
# FIXME: currently a view cannot be defined via eschema
CREATE VIEW SettingView := default::Setting {
   # the settings link is exclusive, so this one is a singleton
   of_group := default::Setting.<settings[IS default::UserGroup]
};


INSERT Setting {
    name := 'template',
    value := 'blue'
};

INSERT Setting {
    name := 'perks',
    value := 'full'
};

INSERT UserGroup {
    name := 'basic'
};

INSERT UserGroup {
    name := 'upgraded',
    settings := (SELECT Setting)
};

INSERT User {
    name := 'John',
    age := 25,
    active := True,
    score := 3.14,
    groups := (SELECT UserGroup FILTER UserGroup.name = 'basic')
};

INSERT User {
    name := 'Jane',
    age := 25,
    active := True,
    score := 1.23,
    groups := (SELECT UserGroup FILTER UserGroup.name = 'upgraded')
};

INSERT User {
    name := 'Alice',
    age := 27,
    active := True,
    score := 5.0,
    profile := (INSERT Profile {
        name := 'Alice profile',
        value := 'special',
        tags := ['1st', '2nd'],
    })
};

INSERT Person {
    name := 'Bob',
    age := 21,
    active := True,
    score := 4.2
};

WITH MODULE other
INSERT Foo {
    `select` := 'a',
    color := <color_enum_t>'RED',
};

WITH MODULE other
INSERT Foo {
    `select` := 'b',
    after := 'w',
    color := <color_enum_t>'GREEN',
};

WITH MODULE other
INSERT Foo {
    after := 'q',
    color := <color_enum_t>'BLUE',
};

INSERT ScalarTest {
    p_bool := True,
    p_str := 'Hello',
    p_datetime := <datetime>'2018-05-07T20:01:22.306916+00:00',
    p_local_datetime := <local_datetime>'2018-05-07T20:01:22.306916',
    p_local_date := <local_date>'2018-05-07',
    p_local_time := <local_time>'20:01:22.306916',
    p_duration := <duration>'20 hrs',
    p_int16 := 12345,
    p_int32 := 1234567890,
    p_int64 := 1234567890123,
    p_float32 := 2.5,
    p_float64 := 2.5,
    p_decimal := <decimal>
        '123456789123456789123456789.123456789123456789123456789',
    p_json := to_json('{"foo": [1, null, "bar"]}'),
    p_bytes := b'Hello World',
};
