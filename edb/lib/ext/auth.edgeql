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

    create abstract type ext::auth::User;

    create type ext::auth::Provider {
        create required property name: std::str;
        create required property url: std::str {
            create constraint exclusive;
        };
    };

    create type ext::auth::Identity {
        create required link issuer: ext::auth::Provider;
        create required link subject: ext::auth::User;

        # Standard claims
        create required property sub: std::str;
        create property name: std::str;
        create property given_name: std::str;
        create property family_name: std::str;
        create property middle_name: std::str;
        create property nickname: std::str;
        create property preferred_username: std::str;
        create property profile: std::str;
        create property picture: std::str;
        create property website: std::str;
        create property gender: std::str;
        create property birthdate: std::str;
        create property zoneinfo: std::str;
        create property locale: std::str;
        create property updated_at: std::datetime;
    };

    create type ext::auth::Email {
        create required property address: std::str;
        create required property verified: std::bool;
        create required property primary: std::bool;

        create required link identity: ext::auth::Identity;

        create constraint exclusive on ((.identity, .primary));
    };

    create type ext::auth::Session {
        create required property token: std::str {
            create constraint exclusive;
        };
        create required property created_at: std::datetime;
        create property expires_at: std::datetime;

        create required link user: ext::auth::User;
    };
};
