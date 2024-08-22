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

CREATE MODULE net;

CREATE SCALAR TYPE net::RequestState EXTENDING std::enum<
    Pending,
    InProgress,
    Completed,
    Failed
>;

CREATE SCALAR TYPE net::RequestFailureKind EXTENDING std::enum<
    NetworkError,
    Timeout
>;

CREATE MODULE net::http;

CREATE SCALAR TYPE net::http::Method EXTENDING std::enum<
    `GET`,
    POST,
    PUT,
    `DELETE`,
    HEAD,
    OPTIONS,
    PATCH
>;

CREATE TYPE net::http::ScheduledRequest extending std::BaseObject {
    CREATE REQUIRED PROPERTY state: net::RequestState;
    CREATE REQUIRED PROPERTY created_at: std::datetime;
    CREATE PROPERTY failure: tuple<kind: net::RequestFailureKind, message: str>;

    CREATE REQUIRED PROPERTY url: str;
    CREATE REQUIRED PROPERTY method: net::http::Method;
    CREATE PROPERTY headers: array<tuple<name: str, value: str>>;
    CREATE PROPERTY body: bytes;

    CREATE LINK response: net::http::Response {
        CREATE CONSTRAINT EXCLUSIVE;
    };
};

CREATE TYPE net::http::Response EXTENDING std::BaseObject {
    CREATE REQUIRED PROPERTY created_at: datetime;
    CREATE PROPERTY status: int16;
    CREATE PROPERTY headers: array<tuple<name: str, value: str>>;
    CREATE PROPERTY body: bytes;
    CREATE PROPERTY request: .<response[is net::http::ScheduledRequest];
};

CREATE FUNCTION
net::http::schedule_request(
    url: str,
    named only body: optional bytes,
    named only method: net::HttpMethod = net::HttpMethod::`GET`,
    named only headers: optional array<tuple<name: str, value: str>>,
) -> net::http::ScheduledRequest
{
    USING SQL $$
        SELECT 42;
    $$
};
