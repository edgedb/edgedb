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

type Post {
    title: str;
    body: str;

    index fts::index on ((
        fts::with_options(
            .title,
            language := fts::Language.eng
        ),
        fts::with_options(
            ext::pg_unaccent::unaccent(.body),
            language := fts::Language.eng,
        ),
    ));
};
