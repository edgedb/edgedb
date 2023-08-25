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

    create type ext::auth::ClientConfig extending cfg::ConfigObject {
        create required property name: std::str {
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
        create property auth_signing_key -> std::str {
            create annotation std::description :=
                "The signing key used for auth extension. Must be at \
                least 32 characters long.";
            set default := "00000000000000000000000000000000";
        };
    };
};
