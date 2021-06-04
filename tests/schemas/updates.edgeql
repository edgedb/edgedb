#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


INSERT Status {
    name := 'Open'
};

INSERT Status {
    name := 'Closed'
};

INSERT MajorLifeEvent {
    name := 'Broke a Type System'
};

INSERT MajorLifeEvent {
    name := 'Downloaded a Car'
};

INSERT Tag {
    name := 'fun'
};

INSERT Tag {
    name := 'boring'
};

INSERT Tag {
    name := 'wow'
};

INSERT UpdateTest {
    name := 'update-test1',
    status := (SELECT Status FILTER Status.name = 'Open'),
    readonly_tag := (SELECT Tag FILTER .name = 'wow'),
    readonly_note := 'this is read-only',
};

INSERT UpdateTest {
    name := 'update-test2',
    comment := 'second',
    status := (SELECT Status FILTER Status.name = 'Open')
};

INSERT UpdateTest {
    name := 'update-test3',
    comment := 'third',
    status := (SELECT Status FILTER Status.name = 'Closed')
};

INSERT CollectionTest {
    name := 'collection-test1'
};
