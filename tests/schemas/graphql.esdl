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
    required property active -> bool {
        default := false;
    }
    multi link groups -> UserGroup;
    required property age -> int64;
    required property score -> float64 {
        default := 0;
    }
    link profile -> Profile;
    # a link pointing to an abstract type
    multi link favorites -> NamedObject;
    multi link fav_users := .favorites[is User];
}

alias SettingAlias := Setting {
    of_group := .<settings[IS UserGroup]
};

alias SettingAliasAugmented := Setting {
    of_group := .<settings[IS UserGroup] {
        name_upper := str_upper(.name)
    }
};

alias ProfileAlias := Profile {
    # although this will point to an actual user, but the type system
    # will only resolve an Object here
    owner := .<profile
};

alias TwoUsers := (
    select User {
        initial := .name[0],
    } order by .name limit 2
);

type Person extending User;

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
    property p_decimal_str := <str>.p_decimal;
    property p_json -> json;
    property p_bytes -> bytes;

    property p_posint -> positive_int_t;
    property p_array_str -> array<str>;
    property p_array_int64 -> array<int64>;
    property p_array_json -> array<json>;
    property p_array_bytes -> array<bytes>;

    property p_tuple -> tuple<int64, str>;
    property p_array_tuple -> array<tuple<str, bool>>;
}
type BigIntTest {
    property value -> bigint;
}

# Inheritance tests
type Bar {
    property q -> str
}

type Bar2 extending Bar {
    property w -> str
}

type Rab {
    # target type will be overridden
    link blah -> Bar
}

type Rab2 extending Rab {
    overloaded link blah -> Bar2;
}

type Genre extending NamedObject {
    multi link games -> Game;
}

type Game extending NamedObject {
    multi link players -> User
}

# Recursive structure
type LinkedList extending NamedObject {
    link next -> LinkedList
}

global test_global_str -> str;
global test_global_array -> array<str>;
global test_global_id -> uuid;
required global test_global_def -> str {
    default := ""
};
optional global test_global_def2 -> str {
    default := ""
};
function get_glob() -> optional str using (global test_global_str);

alias GlobalTest := {
    gstr := global test_global_str,
    garray := global test_global_array,
    gid := global test_global_id,
    gdef := global test_global_def,
    gdef2 := global test_global_def2,
};

function id_func(s: str) -> str using (s);

alias FuncTest := {
    fstr := id_func('test'),
};
