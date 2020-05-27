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
    optional multi link settings -> Setting {
        constraint exclusive;
    }
}

type Setting extending NamedObject {
    required property value -> str;
}

type Profile extending NamedObject {
    required property value -> str;
    optional property tags -> array<str>;
    optional multi property odd -> array<int64>;
}

type User extending NamedObject {
    required property active -> bool;
    optional multi link groups -> UserGroup;
    required property age -> int64;
    required property score -> float64;
    optional link profile -> Profile;
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

scalar type positive_int_t extending int64 {
    constraint min_value(0);
}

type ScalarTest {
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
    optional property p_bigint -> bigint;
    optional property p_float32 -> float32;
    optional property p_float64 -> float64;
    optional property p_decimal -> decimal;
    optional property p_json -> json;
    optional property p_bytes -> bytes;

    optional property p_posint -> positive_int_t;
    optional property p_array_str -> array<str>;
    optional property p_array_int64 -> array<int64>;
    optional property p_array_json -> array<json>;
    optional property p_array_bytes -> array<bytes>;
}
