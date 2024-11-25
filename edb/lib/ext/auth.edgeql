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
    set dependencies := ["pgcrypto>=1.3"];

    create module ext::auth;

    create abstract type ext::auth::Auditable extending std::BaseObject {
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
            on target delete delete source;
        };
    };

    create type ext::auth::EmailFactor extending ext::auth::Factor {
        create required property email: str;
        create property verified_at: std::datetime;
    };

    create type ext::auth::EmailPasswordFactor
        extending ext::auth::EmailFactor {
        alter property email {
            create constraint exclusive;
        };
        create required property password_hash: std::str;
    };

    create type ext::auth::MagicLinkFactor extending ext::auth::EmailFactor {
        alter property email {
            create constraint exclusive;
        };
    };

    create type ext::auth::WebAuthnFactor extending ext::auth::EmailFactor {
        create required property user_handle: std::bytes;
        create required property credential_id: std::bytes {
            create constraint exclusive;
        };
        create required property public_key: std::bytes {
            create constraint exclusive;
        };

        create trigger email_shares_user_handle after insert for each do (
            std::assert(
                __new__.user_handle = (
                    select detached ext::auth::WebAuthnFactor
                    filter .email = __new__.email
                    and not .id = __new__.id
                ).user_handle,
                message := "user_handle must be the same for a given email"
            )
        );
        create constraint exclusive on ((.email, .credential_id));
    };

    create type ext::auth::WebAuthnRegistrationChallenge
        extending ext::auth::Auditable {
        create required property challenge: std::bytes {
            create constraint exclusive;
        };
        create required property email: std::str;
        create required property user_handle: std::bytes;

        create constraint exclusive on ((.user_handle, .email, .challenge));
    };

    create type ext::auth::WebAuthnAuthenticationChallenge
        extending ext::auth::Auditable {
        create required property challenge: std::bytes {
            create constraint exclusive;
        };
        create required multi link factors: ext::auth::WebAuthnFactor {
            create constraint exclusive;
            on target delete delete source;
        };
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
        create property id_token: std::str {
            create annotation std::description :=
                "Identity provider's OpenID Connect id_token.";
        };
        create link identity: ext::auth::Identity {
            on target delete delete source;
        };
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

    create type ext::auth::OpenIDConnectProvider
        extending ext::auth::OAuthProviderConfig {
        alter property name {
            set protected := false;
        };

        alter property display_name {
            set protected := false;
        };

        create required property issuer_url: std::str {
            create annotation std::description :=
                "The issuer URL of the provider.";
        };

        create property logo_url: std::str {
            create annotation std::description :=
                "A url to an image of the provider's logo.";
        };

        create constraint exclusive on ((.issuer_url, .client_id));
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

    create type ext::auth::DiscordOAuthProvider
        extending ext::auth::OAuthProviderConfig {
        alter property name {
            set default := 'builtin::oauth_discord';
        };

        alter property display_name {
            set default := 'Discord';
        };
    };

    create type ext::auth::SlackOAuthProvider
        extending ext::auth::OAuthProviderConfig {
        alter property name {
            set default := 'builtin::oauth_slack';
        };

        alter property display_name {
            set default := 'Slack';
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

    create type ext::auth::WebAuthnProviderConfig
        extending ext::auth::ProviderConfig {
        alter property name {
            set default := 'builtin::local_webauthn';
            set protected := true;
        };

        create required property relying_party_origin: std::str {
            create annotation std::description :=
                "The full origin of the sign-in page including protocol and \
                port of the application. If using the built-in UI, this \
                should be the origin of the EdgeDB server.";
        };

        create required property require_verification: std::bool {
            set default := true;
        };
    };

    create type ext::auth::MagicLinkProviderConfig
        extending ext::auth::ProviderConfig {
        alter property name {
            set default := 'builtin::local_magic_link';
            set protected := true;
        };

        create required property token_time_to_live: std::duration {
            set default := <std::duration>'10 minutes';
            create annotation std::description :=
                "The time after which a magic link token expires.";
        };
    };

    create scalar type ext::auth::FlowType extending std::enum<PKCE, Implicit>;

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

        create required property flow_type: ext::auth::FlowType {
            create annotation std::description :=
                "The flow used when requesting authentication.";
            set default := ext::auth::FlowType.PKCE;
        };

        create property app_name: std::str {
            create annotation std::description :=
                "The name of your application to be shown on the login \
                screen.";
            create annotation std::deprecated :=
                "Use the app_name property in ext::auth::AuthConfig instead.";
        };

        create property logo_url: std::str {
            create annotation std::description :=
                "A url to an image of your application's logo.";
            create annotation std::deprecated :=
                "Use the logo_url property in ext::auth::AuthConfig instead.";
        };

        create property dark_logo_url: std::str {
            create annotation std::description :=
                "A url to an image of your application's logo to be used \
                with the dark theme.";
            create annotation std::deprecated :=
                "Use the dark_logo_url property in ext::auth::AuthConfig \
                instead.";
        };

        create property brand_color: std::str {
            create annotation std::description :=
                "The brand color of your application as a hex string.";
            create annotation std::deprecated :=
                "Use the brand_color property in ext::auth::AuthConfig \
                instead.";
        };
    };

    create scalar type ext::auth::WebhookEvent extending std::enum<
        IdentityCreated,
        IdentityAuthenticated,
        EmailFactorCreated,
        EmailVerified,
        EmailVerificationRequested,
        PasswordResetRequested,
        MagicLinkRequested,
    >;

    create type ext::auth::WebhookConfig extending cfg::ConfigObject {
        create required property url: std::str {
            create annotation std::description :=
                "The url to send webhooks to.";

            create constraint exclusive;
        };

        create required multi property events: ext::auth::WebhookEvent {
            create annotation std::description :=
                "The events to send webhooks for.";
        };

        create property signing_secret_key: std::str {
            set secret := true;

            create annotation std::description :=
                "The secret key used to sign webhook requests.";
        };
    };

    create function ext::auth::webhook_signing_key_exists(
        webhook_config: ext::auth::WebhookConfig
    ) -> std::bool {
        using (
            select exists webhook_config.signing_secret_key
        );
    };

    create type ext::auth::AuthConfig extending cfg::ExtensionConfig {
        create multi link providers: ext::auth::ProviderConfig {
            create annotation std::description :=
                "Configuration for auth provider clients.";
        };

        create link ui: ext::auth::UIConfig {
            create annotation std::description :=
                "Configuration for builtin auth UI. If not set the builtin \
                UI is disabled.";
        };

        create multi link webhooks: ext::auth::WebhookConfig {
            create annotation std::description :=
                "Configuration for webhooks.";
        };

        create property app_name: std::str {
            create annotation std::description :=
                "The name of your application.";
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

        create property auth_signing_key: std::str {
            set secret := true;
            create annotation std::description :=
                "The signing key used for auth extension. Must be at \
                least 32 characters long.";
        };

        create property token_time_to_live: std::duration {
            create annotation std::description :=
                "The time after which an auth token expires. A value of 0 \
                indicates that the token should never expire.";
            set default := <std::duration>'336 hours';
        };

        create multi property allowed_redirect_urls: std::str {
            create annotation std::description :=
                "When redirecting the user in various flows, the URL will be \
                checked against this list to ensure they are going \
                to a trusted domain controlled by the application. URLs are \
                matched based on checking if the candidate redirect URL is \
                a match or a subdirectory of any of these allowed URLs";
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

    create global ext::auth::client_token: std::str;
    create single global ext::auth::ClientTokenIdentity := (
        with
            conf := {
                key := (
                    select cfg::Config.extensions[is ext::auth::AuthConfig]
                    limit 1
                ).auth_signing_key,
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
