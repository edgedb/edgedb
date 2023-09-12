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

    create module ext::auth;

    create type ext::auth::Identity {
        create required property iss: std::str;
        create required property sub: std::str;
        create property email: std::str;

        create constraint exclusive on ((.iss, .sub))
    };

    create type ext::auth::LocalIdentity extending ext::auth::Identity {
        create required property handle: std::str {
            create constraint exclusive;
        };
    };

    create type ext::auth::PasswordCredential {
        create required property password_hash: std::str;
        create required link identity: ext::auth::LocalIdentity {
            create constraint exclusive;
        };
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

        create required property url: std::str {
            set readonly := true;
            create annotation std::description := "Authorization server URL";
        };

        create required property secret: std::str {
            set readonly := true;
            create annotation std::description :=
                "Secret provided by auth provider";
        };

        create required property client_id: std::str {
            set readonly := true;
            create annotation std::description :=
                "ID for client provided by auth provider";
        };
    };

    create type ext::auth::AuthConfig extending cfg::ExtensionConfig {
        create multi link providers -> ext::auth::ClientConfig {
            create annotation std::description :=
                "Configuration for auth provider clients";
        };

        create property auth_signing_key -> std::str {
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
};
