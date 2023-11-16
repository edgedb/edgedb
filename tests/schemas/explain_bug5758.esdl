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


# Schema for bug #5758
type User {
    required name: str;
};

type Album extending TimeTrackedEntity {
    required name: str;

    multi link tracks: Track {
        position: int16;
    };
};

type Track extending TimeTrackedEntity {
    required name: str;
    multi link artists: Artist;
    multi link liked_by: User;
};

type Artist extending TimeTrackedEntity {
    required name: str;
    required stream_count: int64;
    required follower_count: int64;
};

abstract type TimeTrackedEntity {
    created: datetime {
        rewrite insert using (datetime_of_statement())
    };

    modified: datetime {
        rewrite update using (datetime_of_statement())
    };
};