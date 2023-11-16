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


create extension package pgcrypto version '1.3' {
    set ext_module := "ext::pgcrypto";
    set sql_extensions := ["pgcrypto >=1.3"];

    create module ext::pgcrypto;

    create function ext::pgcrypto::digest(
        data: std::str,
        type: std::str,
    ) -> std::bytes {
        set volatility := 'Immutable';
        using sql function 'edgedb.digest';
    };

    create function ext::pgcrypto::digest(
        data: std::bytes,
        type: std::str,
    ) -> std::bytes {
        set volatility := 'Immutable';
        using sql function 'edgedb.digest';
    };

    create function ext::pgcrypto::hmac(
        data: std::str,
        key: std::str,
        type: std::str,
    ) -> std::bytes {
        set volatility := 'Immutable';
        using sql function 'edgedb.hmac';
    };

    create function ext::pgcrypto::hmac(
        data: std::bytes,
        key: std::bytes,
        type: std::str,
    ) -> std::bytes {
        set volatility := 'Immutable';
        using sql function 'edgedb.hmac';
    };

    create function ext::pgcrypto::gen_salt(
    ) -> std::str {
        set volatility := 'Volatile';
        using sql "SELECT edgedb.gen_salt('bf')";
    };

    create function ext::pgcrypto::gen_salt(
        type: std::str,
    ) -> std::str {
        set volatility := 'Volatile';
        using sql 'SELECT edgedb.gen_salt("type")';
    };

    create function ext::pgcrypto::gen_salt(
        type: std::str,
        iter_count: std::int64,
    ) -> std::str {
        set volatility := 'Volatile';
        using sql 'SELECT edgedb.gen_salt("type", "iter_count"::integer)';
    };

    create function ext::pgcrypto::crypt(
        password: std::str,
        salt: std::str,
    ) -> std::str {
        set volatility := 'Immutable';
        using sql function 'edgedb.crypt';
    };
};
