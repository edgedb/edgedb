#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


type Person {
    required first_name: str;
    last_name: str;
}

type Genre {
    required name: str;
}

type Content {
    required title: str;
    genre: Genre;
}

type Movie extending Content {
    release_year: int64;
    multi actors: Person {
        role: str;
    };
    director: Person {
        bar: str;
    };
}

type Book extending Content {
    required pages: int16;
    multi chapters: str;
}

type novel extending Book {
    foo: str;
}

module nested {
    type Hello {
        property hello -> str;
    };

    module deep {
        type Rolling {
            property rolling -> str;
        };
    };
}
