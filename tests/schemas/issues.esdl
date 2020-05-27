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
    required property body -> str {
        # Maximum length of text is 10000 characters.
        constraint max_len_value(10000);
    }
}

abstract type Named {
    required property name -> str;
}

# Dictionary is a NamedObject variant, that enforces
# name uniqueness across all instances if its subclass.
abstract type Dictionary extending Named {
    overloaded required property name -> str {
        delegated constraint exclusive;
    }
    index on (__subject__.name);
}

type User extending Dictionary {
    optional multi link todo -> Issue {
        optional property rank -> int64 {
            default := 42;
        }
    }
}

abstract type Owned {
    # By default links are optional.
    required link owner -> User {
        optional property note -> str;
    }
}

type Status extending Dictionary;

type Priority extending Dictionary;

type LogEntry extending Owned, Text {
    # LogEntry is an Owned and a Text, so it
    # will have all of their links and attributes,
    # in particular, owner and text links.
    required property spent_time -> int64;
}

scalar type issue_num_t extending std::str;

type Comment extending Text, Owned {
    required link issue -> Issue;
    optional link parent -> Comment;
}

type Issue extending Named, Owned, Text {
    overloaded required link owner {
        optional property since -> datetime;
    }

    required property number -> issue_num_t {
        readonly := true;
        constraint exclusive;
    }
    required link status -> Status;

    optional link priority -> Priority;

    optional multi link watchers -> User;

    optional property time_estimate -> int64;

    optional multi link time_spent_log -> LogEntry;

    optional property start_date -> datetime {
        default := (SELECT datetime_current());
        # The default value of start_date will be a
        # result of the EdgeQL expression above.
    }
    optional property due_date -> datetime;

    optional multi link related_to -> Issue;

    optional multi link references -> File | URL | Publication {
        optional property list_order -> int64;
    };

    optional property tags -> array<str>;
}

type File extending Named;

type URL extending Named {
    required property address -> str;
}

type Publication {
    required property title -> str;
    optional multi link authors -> User {
        optional property list_order -> int64;
    };
}

abstract constraint my_one_of(one_of: array<anytype>) {
    using (contains(one_of, __subject__));
}

scalar type EmulatedEnum extending str {
    constraint one_of('v1', 'v2');
}
