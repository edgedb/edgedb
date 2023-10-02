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


create extension package smtp version '1.0' {
    set ext_module := "ext::smtp";

    create scalar type ext::smtp::ConnectionSecurity extending enum<PlainText, TLS, STARTTLS, PreferSTARTTLS>;

    create type ext::smtp::ServerConfig extending cfg::ExtensionConfig {
        create property host -> std::str;
        create property port -> std::int32;
        create property username -> std::str;
        create property password -> std::str {
            set secret := true;
        };
        create required property security -> ext::smtp::ConnectionSecurity {
            set default := ext::smtp::ConnectionSecurity.PreferSTARTTLS;
        };
        create required property validate_certs -> std::bool {
            set default := true;
        };
        create required property concurrency -> std::int32 {
            create constraint std::min_value(1);
            set default := 2;
        };
        create required property timeout_per_email -> std::duration {
            set default := <std::duration>'60 seconds';
        };
        create required property timeout_per_attempt -> std::duration {
            set default := <std::duration>'15 seconds';
        };
    };
};
