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


CREATE EXTENSION PACKAGE auth VERSION '1.0' {
    set ext_module := "ext::auth";
    set dependencies := ["pgcrypto==1.3"];

    create module ext::auth;

    create abstract type ext::auth::Auditable {
        create required property created_at: std::datetime {
            set default := std::datetime_current();
            set readonly := true;
        };
        create required property modified_at: std::datetime {
            create rewrite insert, update using (
                std::datetime_current()
            );
        };
    };

    create type ext::auth::Identity extending ext::auth::Auditable {
        create required property issuer: std::str;
        create required property subject: std::str;

        create constraint exclusive on ((.issuer, .subject));
    };

    create type ext::auth::LocalIdentity extending ext::auth::Identity {
        alter property subject {
            create rewrite insert using (<str>.id);
        };
    };

    create abstract type ext::auth::Factor extending ext::auth::Auditable {
        create required link identity: ext::auth::LocalIdentity {
            create constraint exclusive;
        };
    };

    create type ext::auth::EmailFactor extending ext::auth::Factor {
        create required property email: str {
            create delegated constraint exclusive;
        };
    };

    create type ext::auth::EmailPasswordFactor
        extending ext::auth::EmailFactor {
        create required property password_hash: std::str;
    };

    create type ext::auth::PKCEChallenge extending ext::auth::Auditable {
        create required property challenge: std::str {
            create constraint exclusive;
        };
        create link identity: ext::auth::Identity;
    };

    create type ext::auth::ClientConfig extending cfg::ConfigObject {
        create required property provider_id: std::str {
            set readonly := true;
            create annotation std::description :=
                "ID of the auth provider";
            create constraint exclusive;
        };

        create required property provider_name: std::str {
            set readonly := true;
            create annotation std::description := "Auth provider name";
        };
    };

    create type ext::auth::OAuthClientConfig
        extending ext::auth::ClientConfig {
        create required property url: std::str {
            set readonly := true;
            create annotation std::description := "Authorization server URL";
        };

        create required property secret: std::str {
            set readonly := true;
            set secret := true;
            create annotation std::description :=
                "Secret provided by auth provider";
        };

        create required property client_id: std::str {
            set readonly := true;
            create annotation std::description :=
                "ID for client provided by auth provider";
        };
    };

    create type ext::auth::PasswordClientConfig
        extending ext::auth::ClientConfig;

    create type ext::auth::AuthConfig extending cfg::ExtensionConfig {
        create multi link providers -> ext::auth::ClientConfig {
            create annotation std::description :=
                "Configuration for auth provider clients";
        };

        create property auth_signing_key -> std::str {
            set secret := true;
            create annotation std::description :=
                "The signing key used for auth extension. Must be at \
                least 32 characters long.";
        };

        create property token_time_to_live -> std::duration {
            create annotation std::description :=
                "The time after which an auth token expires. A value of 0 \
                indicates that the token should never expire.";
            set default := <std::duration>'336 hours';
        };
    };

    create function ext::auth::signing_key_exists() -> std::bool {
        using (
            select exists cfg::Config.extensions[is ext::auth::AuthConfig]
                .auth_signing_key
        );
    };

    create scalar type ext::auth::JWTAlgo extending enum<RS256, HS256>;

    create function ext::auth::_jwt_check_signature(
        jwt: tuple<header: std::str, payload: std::str, signature: std::str>,
        key: std::str,
        algo: ext::auth::JWTAlgo = ext::auth::JWTAlgo.HS256,
    ) -> std::json
    {
        set volatility := 'Stable';
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
        set volatility := 'Stable';
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
        algo: ext::auth::JWTAlgo = ext::auth::JWTAlgo.HS256,
    ) -> std::json
    {
        set volatility := 'Stable';
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
            conf := {
                key := (
                    cfg::Config.extensions[is ext::auth::AuthConfig]
                    .auth_signing_key
                ),
            },
            jwt := {
                claims := ext::auth::_jwt_verify(
                    global ext::auth::client_token,
                    conf.key,
                )
            },
        select
            ext::auth::Identity
        filter
            .id = <uuid>json_get(jwt.claims, "sub")
    );
};
