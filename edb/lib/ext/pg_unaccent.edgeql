#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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


create extension package pg_unaccent version '1.1' {
    set ext_module := "ext::pg_unaccent";
    set sql_extensions := ["unaccent >=1.1"];

    create module ext::pg_unaccent;

    create function ext::pg_unaccent::unaccent(
        text: std::str,
    ) -> std::str {
        set volatility := 'Immutable';
        using sql 'select edgedb.unaccent(text)';
    };
};
