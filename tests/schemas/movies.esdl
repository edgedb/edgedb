#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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

global username_prefix: str;

type Person {
    required first_name: str;
    last_name: str;

    full_name := __source__.first_name ++ ((' ' ++ .last_name) ?? '');
    favorite_genre := (select Genre filter .name = 'Drama' limit 1);
    username := (global username_prefix ?? 'u_') ++ str_lower(.first_name);
}

type Genre {
    required name: str;
}

global filter_title: str;

type Content {
    required title: str;
    genre: Genre;

    access policy filter_title
        allow select
        using (global filter_title ?= .title);
    access policy dml allow insert, update, delete;
}

type Movie extending Content {
    release_year: int64;
    multi actors: Person {
        property role: str;
        property role_lower := str_lower(@role);
    };
    director: Person {
        bar: str;
    };

    multi actor_names := __source__.actors.first_name;
    multi similar_to := (select Content);
}

type Book extending Content {
    required pages: int16;
    multi chapters: str;
}

type novel extending Book {
    foo: str;
}

module nested {
    type Hello {
        property hello -> str;
    };

    module deep {
        type Rolling {
            property rolling -> str;
        };
    };
}

type ContentSummary {
    property x := std::count((select Content));

    access policy select_always allow select;
    access policy dml allow insert, update, delete
        using (global filter_title ?= 'summary');
}

module links {
    type A;

    type B {
        multi link a: A;
        link prop: A {
            property lp: str;
        };
        multi property vals: str;
    }

    type C extending B;
}