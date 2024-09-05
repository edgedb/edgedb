#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


abstract type Text {
    # This is an abstract object containing text.
    required body: str {
        # Maximum length of text is 10000 characters.
        constraint max_len_value(10000);
    }
}

abstract type Named {
    required name: str;
}

# Dictionary is a NamedObject variant, that enforces
# name uniqueness across all instances if its subclass.
abstract type Dictionary extending Named {
    overloaded required name: str {
        delegated constraint exclusive;
    }
    index on (.name);
}

type User extending Dictionary {
    multi todo: Issue {
        rank: int64 {
            default := 42;
        }
    }
}

abstract type Owned {
    # By default links are optional.
    required owner: User {
        note: str;
    }
}

type Status extending Dictionary;

type Priority extending Dictionary;

type LogEntry extending Owned, Text {
    # LogEntry is an Owned and a Text, so it
    # will have all of their links and attributes,
    # in particular, owner and text links.
    required spent_time: int64;
}

scalar type issue_num_t extending std::str;

type Comment extending Text, Owned {
    required issue: Issue;
    optional parent: Comment;
}

type Issue extending Named, Owned, Text {
    overloaded required link owner {
        property since: datetime;
    }

    required number: issue_num_t {
        readonly := true;
        constraint exclusive;
    }
    required status: Status;

    priority: Priority;

    optional multi watchers: User;

    optional time_estimate: int64;

    multi time_spent_log: LogEntry;

    start_date: datetime {
        default := (SELECT datetime_current());
        # The default value of start_date will be a
        # result of the EdgeQL expression above.
    }
    due_date: datetime;

    multi related_to: Issue;

    multi references: File | URL | Publication {
        list_order: int64;
    };

    tags: array<str>;

    index fts::index on ((
        fts::with_options(.name, language := fts::Language.eng),
        fts::with_options(.body, language := fts::Language.eng),
    ));
}

# This is used to test correct behavior of boolean operators: NOT,
# AND, OR. It targets especially interactions of properties and {}.
#
# Issue can be used to test similar interaction for links.
type BooleanTest extending Named {
    val: int64;
    multi tags: str;
}

type File extending Named;

type URL extending Named {
    required address: str;
}

type Publication {
    required title: str;

    title1 := (SELECT ident(.title));
    required title2 := (SELECT ident(.title));
    required single title3 := (SELECT ident(.title));
    optional single title4 := (SELECT ident(.title));
    optional multi title5 := (SELECT ident(.title));
    required multi title6 := (SELECT ident(.title));

    multi authors: User {
        list_order: int64;
    };
}

abstract constraint my_one_of(one_of: array<anytype>) {
    using (contains(one_of, __subject__));
}

scalar type EmulatedEnum extending str {
    constraint one_of('v1', 'v2');
}

function ident(a: str) -> str {
    USING (SELECT a)
}


function opt_test(tag: int64, x: str) -> str using (x ?? '');
function opt_test(tag: bool, x: optional str) -> str using (x ?? '');
function opt_test(tag: int64, x: int64) -> int64 using (x ?? -1);
function opt_test(tag: bool, x: optional int64) -> int64 using (x ?? -1);

function opt_test(tag: int64, x: int64, y: optional int64) -> int64 using (y ?? -1);
function opt_test(tag: bool, x: optional int64, y: optional int64) -> int64 using (y ?? -1);

function all_objects() -> SET OF BaseObject {
    USING (BaseObject)
}
