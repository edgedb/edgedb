#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

insert Genre { name:= 'Fiction' };
insert Genre { name:= 'Drama' };
insert Genre { name:= '武侠' };

insert Person { first_name:= 'Tom', last_name:= 'Hanks' };
insert Person { first_name:= 'Robin' };
insert Person { first_name:= 'Steven', last_name:= 'Spielberg' };

insert Movie {
    title := 'Forrest Gump',
    release_year := 1994,
    actors := (select Person
        filter .first_name = 'Tom' or .first_name = 'Robin'
    ),
    genre := (select Genre filter .name = 'Drama' limit 1),
};

insert Movie {
    title := 'Saving Private Ryan',
    release_year := 1998,
    actors := (
        select Person { @role := 'Captain Miller' } filter .first_name = 'Tom'
    ),
    director := (
        select Person { @bar := 'bar' } filter .last_name = 'Spielberg' limit 1
    ),
    genre := (select Genre filter .name = 'Drama' limit 1),
};

insert novel {
    title:='Hunger Games',
    pages := 374,
    genre:= (select Genre filter .name = 'Fiction' limit 1)
};

insert Book {
    title:='Chronicles of Narnia',
    pages := 206,
    chapters := {
        'Lucy looks into a wardrobe',
        'What Lucy found there',
        'Edmund and the wardrobe',
        'Turkish delight',
    },
    genre:= (select Genre filter .name = 'Fiction' limit 1)
};

insert Content {
    title := 'Halo 3',
    genre := (select Genre filter .name = 'Fiction' limit 1)
};

set global filter_title := 'summary';
insert ContentSummary;
reset global filter_title;

insert default::links::C {
    a := {(insert default::links::A), (insert default::links::A)},
    prop := (insert default::links::A),
    vals := {"1", "2", "3", "4"},
};
