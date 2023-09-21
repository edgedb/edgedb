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


abstract type NamedObject {
    annotation description := 'An object with a name';

    required property name -> str;
}

type UserGroup extending NamedObject {
    multi link settings -> Setting {
        constraint exclusive;
    }
}

type Setting extending NamedObject {
    required property value -> str;
}

type Profile extending NamedObject {
    required property value -> str;
    property tags -> array<str>;
    multi property odd -> array<int64>;

    # computed link and property test
    link owner_user := .<profile[IS User];
    property owner_name := .<profile[IS User].name;
}

type User extending NamedObject {
    required property active -> bool;
    multi link groups -> UserGroup;
    required property age -> int64;
    required property score -> float64;
    link profile -> Profile;
}

alias SettingAlias := Setting {
    of_group := .<settings[IS UserGroup]
};

alias SettingAliasAugmented := Setting {
    of_group := .<settings[IS UserGroup] {
        name_upper := str_upper(.name)
    }
};

type Person extending User;

type Combo {
    # Test type union, Setting and Profile share some inherited fields as well
    # as non-inherited ones.
    link data -> Setting | Profile;
}

scalar type positive_int_t extending int64 {
    constraint min_value(0);
}

type ScalarTest {
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
    property p_bigint -> bigint;
    property p_float32 -> float32;
    property p_float64 -> float64;
    property p_decimal -> decimal;
    property p_json -> json;
    property p_bytes -> bytes;

    property p_posint -> positive_int_t;
    property p_array_str -> array<str>;
    property p_array_int64 -> array<int64>;
    property p_array_json -> array<json>;
    property p_array_bytes -> array<bytes>;
}

type Fixed {
    property computed := 123;
}

type NotEditable {
    property computed := 'a computed value';
    required once: str {
        readonly := true;
    }
}