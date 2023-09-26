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
    index fts::index on (());
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

type Sentence extending Ordered {
    required property text -> str;
    # index not overridden
}

# This is intended to test the various FTS schema features.
type Text {
    required text: str;
    index fts::index on (fts::with_options(.text,
        language := fts::Language.eng,
        weight_category := fts::Weight.A
    ));
}

type FancyText extending Text {
    required style: int64;
}

type QuotedText extending Text {
    required author: str;
}

type FancyQuotedText extending FancyText, QuotedText;

type TitledText extending Text {
    required title: str;
    index fts::index on ((
        fts::with_options(
            .title,
            language := fts::Language.eng
        ),
        fts::with_options(
            .text,
            language := fts::Language.eng
        )
    ));
}


type Post {
    required title: str;
    body: str;
    # 2 properties are subject to FTS with different weights
    index fts::index on ((
        fts::with_options(.title,
            language := fts::Language.eng,
            weight_category := fts::Weight.A
        ),
        fts::with_options(.body,
            language := fts::Language.eng,
            weight_category := fts::Weight.B
        )
    ));

    note: str;
    weight_a: float64;
}

type Description {
    required num: int64;
    required raw: str;
    required property text := 'Item #' ++ to_str(.num) ++ ': ' ++ .raw;
    # FTS on a computed property
    index fts::index on (
        fts::with_options(.text,
            language := fts::Language.eng,
            weight_category := fts::Weight.C
        )
    );
}

type MultiLang {
    required eng: str;
    required fra: str;
    required ita: str;
    index fts::index on ((
        fts::with_options(.eng,
            language := fts::Language.eng,
            weight_category := fts::Weight.A
        ),
        fts::with_options(.fra,
            language := fts::Language.fra,
            weight_category := fts::Weight.B
        ),
        fts::with_options(.ita,
            language := fts::Language.ita,
            weight_category := fts::Weight.C
        ),
    ));
}

type DynamicLang {
    required text: str;
    required lang: fts::Language;
    index fts::index on (
        fts::with_options(.text,
            language := .lang,
            weight_category := fts::Weight.A
        )
    );
}

type TouristVocab {
    required text: str;
    index fts::index on ((
        fts::with_options(str_split(.text, '--')[0],
            language := fts::Language.eng,
            weight_category := fts::Weight.A
        ),
        fts::with_options(str_split(.text, '--')[1],
            language := fts::Language.ita,
            weight_category := fts::Weight.B
        ),
    ));
}