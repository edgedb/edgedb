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


INSERT Status {
    name := 'Open'
};

INSERT Status {
    name := 'Closed'
};


INSERT User {
    name := 'Elvis'
};

INSERT User {
    name := 'Yury'
};

INSERT User {
    name := 'Victor'
};


INSERT Issue {
    number := '1',
    name := 'Implicit path existence',
    body := 'Any expression involving paths also implies paths exist.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    time_estimate := 9001,
};

INSERT Issue {
    number := '2',
    name := 'NOT EXISTS problem',
    body := 'Implicit path existence does not apply to NOT EXISTS.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    due_date := <datetime>'2020-01-15T00:00:00+00:00',
};

INSERT Issue {
    number := '3',
    name := 'EdgeQL to SQL translator',
    body := 'Rewrite and refactor translation to SQL.',
    owner := (SELECT User FILTER User.name = 'Yury'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    time_estimate := 9999,
    due_date := <datetime>'2020-01-15T00:00:00+00:00',
};

INSERT Issue {
    number := '4',
    name := 'Translator optimization',
    body := 'At some point SQL translations should be optimized.',
    owner := (SELECT User FILTER User.name = 'Yury'),
    status := (SELECT Status FILTER Status.name = 'Open'),
};
