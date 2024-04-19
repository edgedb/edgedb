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
    index on (__subject__.name);
}

type User extending Dictionary {
    multi todo: Issue {
        rank: int64 {
            default := 42;
        }
    }
    multi link owned_issues := .<owner[is Issue];
}

abstract type Owned {
    # By default links are optional.
    required owner: User {
        note: str;
    }
}

type Status extending Dictionary;

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

function frob(s: str) -> str using (s ++ '!');

type Issue extending Named, Owned, Text {
    overloaded required link owner {
        property since: datetime;
    }

    required number: issue_num_t {
        readonly := true;
        constraint exclusive;
    }
    required status: Status;

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

    multi references: File | URL {
        list_order: int64;
    };

    # Pure index testing stuff
    number2 := frob(.number);
    index on (.number2);
}

type File extending Named;

type URL extending Named {
    required address: str;
}

type RangeTest {
    required rval: range<int64>;
    required mval: multirange<int64>;
    required rdate: range<cal::local_date>;
    required mdate: multirange<cal::local_date>;

    index pg::gist on (.rval);
    index pg::gist on (.mval);
    index pg::gist on (.rdate);
    index pg::gist on (.mdate);
}

type JSONTest {
    required val: json;

    index pg::gin on (.val);
}
