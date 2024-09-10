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

CREATE MODULE std::net;

CREATE SCALAR TYPE std::net::RequestState EXTENDING std::enum<
    Pending,
    InProgress,
    Completed,
    Failed
>;

CREATE SCALAR TYPE std::net::RequestFailureKind EXTENDING std::enum<
    NetworkError,
    Timeout
>;

CREATE MODULE std::net::http;

CREATE SCALAR TYPE std::net::http::Method EXTENDING std::enum<
    `GET`,
    POST,
    PUT,
    `DELETE`,
    HEAD,
    OPTIONS,
    PATCH
>;

CREATE TYPE std::net::http::Response EXTENDING std::BaseObject {
    CREATE REQUIRED PROPERTY created_at: std::datetime;
    CREATE PROPERTY status: std::int16;
    CREATE PROPERTY headers: std::array<std::tuple<name: std::str, value: std::str>>;
    CREATE PROPERTY body: std::bytes;
};

CREATE TYPE std::net::http::ScheduledRequest extending std::BaseObject {
    CREATE REQUIRED PROPERTY state: std::net::RequestState;
    CREATE REQUIRED PROPERTY created_at: std::datetime;
    CREATE PROPERTY failure: tuple<kind: std::net::RequestFailureKind, message: str>;

    CREATE REQUIRED PROPERTY url: std::str;
    CREATE REQUIRED PROPERTY method: std::net::http::Method;
    CREATE PROPERTY headers: std::array<std::tuple<name: std::str, value: std::str>>;
    CREATE PROPERTY body: std::bytes;

    CREATE LINK response: std::net::http::Response {
        CREATE CONSTRAINT exclusive;
    };
};

ALTER TYPE std::net::http::Response {
    CREATE LINK request := .<response[is std::net::http::ScheduledRequest];
};

CREATE FUNCTION
std::net::http::schedule_request(
    url: str,
    named only body: optional std::bytes,
    named only method: std::net::http::Method = std::net::http::Method.`GET`,
    named only headers: optional std::array<std::tuple<name: std::str, value: std::str>>,
) -> std::int16
{
    USING SQL $$
        SELECT 42;
    $$
};
