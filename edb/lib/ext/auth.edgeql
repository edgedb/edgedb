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


CREATE EXTENSION PACKAGE auth VERSION '1.0' {
    set ext_module := "ext::auth";

    create module ext::auth;

    create type ext::auth::Identity {
        create required property iss: std::str;
        create required property sub: std::str;
        create property email: std::str;

        create constraint exclusive on ((.iss, .sub))
    };

    create type ext::auth::AuthConfig extending cfg::ExtensionConfig {
        create property auth_signing_key -> std::str {
            create annotation std::description :=
                'The signing key used for auth extension. Must be at \
                least 32 characters long.';
            set default := '00000000000000000000000000000000';
        };

        create property github_client_secret -> std::str {
            create annotation std::description := 'Secret key provided by GitHub';
            set default := '00000000000000000000000000000000';
        };

        create property github_client_id -> std::str {
            create annotation std::description := 'ID provided by GitHub';
            set default := '00000000000000000000000000000000';
        };
    };

    create scalar type ext::auth::JWTAlgo extending enum<RS256, HS256>;

    create function ext::auth::_jwt_check_signature(
        jwt: tuple<header: std::str, payload: std::str, signature: std::str>,
        key: std::str,
        algo: ext::auth::JWTAlgo = ext::auth::JWTAlgo.RS256,
    ) -> std::json
    {
        set volatility := 'Volatile';  # work-around due to assert()
        using (
            with
                module ext::auth,
                msg := jwt.header ++ "." ++ jwt.payload,
                hash := (
                    "sha256" if algo = JWTAlgo.RS256 or algo = JWTAlgo.HS256
                    else <str>std::assert(
                        false, message := "unsupported JWT algo")
                ),
            select
                std::to_json(
                    std::to_str(
                        std::enc::base64_decode(
                            jwt.payload,
                            padding := false,
                            alphabet := std::enc::Base64Alphabet.urlsafe,
                        ),
                    ),
                )
            order by
                assert(
                    std::enc::base64_encode(
                        ext::pgcrypto::hmac(msg, key, hash),
                        padding := false,
                        alphabet := std::enc::Base64Alphabet.urlsafe,
                    ) = jwt.signature,
                    message := "JWT signature mismatch",
                )
        );
    };

    create function ext::auth::_jwt_parse(
        token: std::str,
    ) -> tuple<header: std::str, payload: std::str, signature: std::str>
    {
        set volatility := 'Volatile';  # work-around due to assert()
        using (
            with
                parts := std::str_split(token, "."),
            select
                (
                    header := parts[0],
                    payload := parts[1],
                    signature := parts[2],
                )
            order by
                assert(len(parts) = 3, message := "JWT is malformed")
        );
    };

    create function ext::auth::_jwt_verify(
        token: std::str,
        key: std::str,
        algo: ext::auth::JWTAlgo = ext::auth::JWTAlgo.RS256,
    ) -> std::json
    {
        set volatility := 'Volatile';  # work-around due to assert()
        using (
            with
                # NB: Free-object wrapping to force materialization
                jwt := {
                    t := ext::auth::_jwt_check_signature(
                        ext::auth::_jwt_parse(token),
                        key,
                        algo,
                    ),
                },
                validity_range := std::range(
                    std::to_datetime(<float64>json_get(jwt.t, "nbf")),
                    std::to_datetime(<float64>json_get(jwt.t, "exp")),
                ),
            select
                jwt.t
            order by
                assert(
                    std::contains(
                        validity_range,
                        std::datetime_of_transaction(),
                    ),
                    message := "JWT is expired or is not yet valid",
                )
        );
    };

    create global ext::auth::client_token -> std::str;
    create global ext::auth::ClientTokenIdentity := (
        with
            conf := cfg::Config.extensions[is ext::auth::AuthConfig],
            jwt := {
                claims := ext::auth::_jwt_verify(
                    global ext::auth::client_token,
                    conf.auth_signing_key,
                )
            },
        select
            ext::auth::Identity
        filter
            .iss = <str>json_get(jwt.claims, "iss")
            and .sub = <str>json_get(jwt.claims, "sub")
    );
};
