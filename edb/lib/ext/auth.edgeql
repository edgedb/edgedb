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
        create property verified_at: std::datetime;
    };

    create type ext::auth::EmailPasswordFactor
        extending ext::auth::EmailFactor {
        create required property password_hash: std::str;
    };

    create type ext::auth::PKCEChallenge extending ext::auth::Auditable {
        create required property challenge: std::str {
            create constraint exclusive;
        };
        create property auth_token: std::str {
            create annotation std::description :=
                "Identity provider's auth token.";
        };
        create property refresh_token: std::str {
            create annotation std::description :=
                "Identity provider's refresh token.";
        };
        create link identity: ext::auth::Identity;
    };

    create abstract type ext::auth::ProviderConfig
        extending cfg::ConfigObject {
        create required property name: std::str {
            set readonly := true;
            create constraint exclusive;
        }
    };

    create abstract type ext::auth::OAuthProviderConfig
        extending ext::auth::ProviderConfig {
        alter property name {
            set protected := true;
        };

        create required property secret: std::str {
            set readonly := true;
            set secret := true;
            create annotation std::description :=
                "Secret provided by auth provider.";
        };

        create required property client_id: std::str {
            set readonly := true;
            create annotation std::description :=
                "ID for client provided by auth provider.";
        };

        create required property display_name: std::str {
            set readonly := true;
            set protected := true;
            create annotation std::description :=
                "Provider name to be displayed in login UI.";
        };

        create property additional_scope: std::str {
            set readonly := true;
            create annotation std::description :=
                "Space-separated list of scopes to be included in the \
                authorize request to the OAuth provider.";
        };
    };

    create type ext::auth::AppleOAuthProvider
        extending ext::auth::OAuthProviderConfig {
        alter property name {
            set default := 'builtin::oauth_apple';
        };

        alter property display_name {
            set default := 'Apple';
        };
    };

    create type ext::auth::AzureOAuthProvider
        extending ext::auth::OAuthProviderConfig {
        alter property name {
            set default := 'builtin::oauth_azure';
        };

        alter property display_name {
            set default := 'Azure';
        };
    };

    create type ext::auth::GitHubOAuthProvider
        extending ext::auth::OAuthProviderConfig {
        alter property name {
            set default := 'builtin::oauth_github';
        };

        alter property display_name {
            set default := 'GitHub';
        };
    };

    create type ext::auth::GoogleOAuthProvider
        extending ext::auth::OAuthProviderConfig {
        alter property name {
            set default := 'builtin::oauth_google';
        };

        alter property display_name {
            set default := 'Google';
        };
    };

    create type ext::auth::EmailPasswordProviderConfig
        extending ext::auth::ProviderConfig {
        alter property name {
            set default := 'builtin::local_emailpassword';
            set protected := true;
        };

        create required property require_verification: std::bool {
            set default := true;
        };
    };

    create type ext::auth::UIConfig extending cfg::ConfigObject {
        create required property redirect_to: std::str {
            create annotation std::description :=
                "The url to redirect to after successful sign in.";
        };

        create property redirect_to_on_signup: std::str {
            create annotation std::description :=
                "The url to redirect to after a new user signs up. \
                If not set, 'redirect_to' will be used instead.";
        };

        create property app_name: std::str {
            create annotation std::description :=
                "The name of your application to be shown on the login \
                screen.";
        };

        create property logo_url: std::str {
            create annotation std::description :=
                "A url to an image of your application's logo.";
        };

        create property dark_logo_url: std::str {
            create annotation std::description :=
                "A url to an image of your application's logo to be used \
                with the dark theme.";
        };

        create property brand_color: std::str {
            create annotation std::description :=
                "The brand color of your application as a hex string.";
        };
    };

    create type ext::auth::AuthConfig extending cfg::ExtensionConfig {
        create multi link providers -> ext::auth::ProviderConfig {
            create annotation std::description :=
                "Configuration for auth provider clients.";
        };

        create link ui -> ext::auth::UIConfig {
            create annotation std::description :=
                "Configuration for builtin auth UI. If not set the builtin \
                UI is disabled.";
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

    create scalar type ext::auth::SMTPSecurity extending enum<PlainText, TLS, STARTTLS, STARTTLSOrPlainText>;

    create type ext::auth::SMTPConfig extending cfg::ExtensionConfig {
        create property sender -> std::str {
            create annotation std::description :=
                "\"From\" address of system emails sent for e.g. \
                password reset, etc.";
        };
        create property host -> std::str {
            create annotation std::description :=
                "Host of SMTP server to use for sending emails. \
                If not set, \"localhost\" will be used.";
        };
        create property port -> std::int32 {
            create annotation std::description :=
                "Port of SMTP server to use for sending emails. \
                If not set, common defaults will be used depending on security: \
                465 for TLS, 587 for STARTTLS, 25 otherwise.";
        };
        create property username -> std::str {
            create annotation std::description :=
                "Username to login as after connected to SMTP server.";
        };
        create property password -> std::str {
            set secret := true;
            create annotation std::description :=
                "Password for login after connected to SMTP server.";
        };
        create required property security -> ext::auth::SMTPSecurity {
            set default := ext::auth::SMTPSecurity.STARTTLSOrPlainText;
            create annotation std::description :=
                "Security mode of the connection to SMTP server. \
                By default, initiate a STARTTLS upgrade if supported by the \
                server, or fallback to PlainText.";
        };
        create required property validate_certs -> std::bool {
            set default := true;
            create annotation std::description :=
                "Determines if SMTP server certificates are validated.";
        };
        create required property timeout_per_email -> std::duration {
            set default := <std::duration>'60 seconds';
            create annotation std::description :=
                "Maximum time to send an email, including retry attempts.";
        };
        create required property timeout_per_attempt -> std::duration {
            set default := <std::duration>'15 seconds';
            create annotation std::description :=
                "Maximum time for each SMTP request.";
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
