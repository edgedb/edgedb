#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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


abstract type Ordered {
    required property number -> int64;
}

type Chapter extending Ordered {
    required property title -> str;

    multi link paragraphs := .<chapter[is Paragraph];

    index fts::index on (
        fts::with_options(.title, language := fts::Language.eng)
    );
}

type Paragraph extending Ordered {
    required link chapter -> Chapter;

    required property text -> str;

    index fts::index on (
        fts::with_options(.text, language := fts::Language.eng)
    );
}
