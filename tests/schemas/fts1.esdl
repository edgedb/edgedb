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


type Text {
    required text: str;
    index fts::index on (fts::with_options(.text,
        language := fts::Language.eng,
        weight_category := 'A'
    ));
}

type FancyText extending Text {
    required style: int64;
}

type QuotedText extending Text {
    required author: str;
}

type FancyQuotedText extending FancyText, QuotedText;

type Post {
    required title: str;
    body: str;
    # 2 properties are subject to FTS
    index fts::index on ((
        fts::with_options(.title,
            language := fts::Language.eng,
            weight_category := 'A'
        ),
        fts::with_options(.body,
            language := fts::Language.eng,
            weight_category := 'B'
        )
    ));
}

type Description {
    required num: int64;
    required raw: str;
    required property text := 'Item #' ++ to_str(.num) ++ ': ' ++ .raw;
    # FTS on a computed property
    index fts::index on (
        fts::with_options(.text,
            language := fts::Language.eng,
            weight_category := 'A'
        )
    );
}
