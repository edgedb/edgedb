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


WITH MODULE test
INSERT Priority {
    name := 'High'
};

WITH MODULE test
INSERT Priority {
    name := 'Low'
};

WITH MODULE test
INSERT Status {
    name := 'Open'
};

WITH MODULE test
INSERT Status {
    name := 'Closed'
};


WITH MODULE test
INSERT User {
    name := 'Elvis'
};

WITH MODULE test
INSERT User {
    name := 'Yury'
};

WITH MODULE test
INSERT URL {
    name := 'edgedb.com',
    address := 'https://edgedb.com'
};

WITH MODULE test
INSERT File {
    name := 'screenshot.png'
};

WITH MODULE test
INSERT LogEntry {
    owner := (
        SELECT
            User {
                @note := 'reassigned'
            }
        FILTER
            User.name = 'Elvis'
    ),
    spent_time := 50000,
    body := 'Rewriting everything.'
};

WITH MODULE test
INSERT Issue {
    number := '1',
    name := 'Release EdgeDB',
    body := 'Initial public release of EdgeDB.',
    owner := (SELECT User {
                @since := <datetime>'2018-01-01T00:00+00',
                @note := 'automatic assignment',
              }
              FILTER User.name = 'Elvis'),
    watchers := (SELECT User FILTER User.name = 'Yury'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    time_spent_log := (SELECT LogEntry),
    time_estimate := 3000
};

WITH MODULE test
INSERT Comment {
    body := 'EdgeDB needs to happen soon.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    issue := (SELECT Issue FILTER Issue.number = '1')
};


WITH MODULE test
INSERT Issue {
    number := '2',
    name := 'Improve EdgeDB repl output rendering.',
    body := 'We need to be able to render data in tabular format.',
    owner := (SELECT User FILTER User.name = 'Yury'),
    watchers := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    priority := (SELECT Priority FILTER Priority.name = 'High'),
    references :=
        (SELECT URL FILTER URL.address = 'https://edgedb.com')
        UNION
        (SELECT File FILTER File.name = 'screenshot.png')
};

WITH
    MODULE test,
    I := DETACHED Issue
INSERT Issue {
    number := '3',
    name := 'Repl tweak.',
    body := 'Minor lexer tweaks.',
    owner := (SELECT User FILTER User.name = 'Yury'),
    watchers := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    related_to := (
        SELECT I FILTER I.number = '2'
    ),
    priority := (SELECT Priority FILTER Priority.name = 'Low')
};

WITH
    MODULE test,
    I := DETACHED Issue
INSERT Issue {
    number := '4',
    name := 'Regression.',
    body := 'Fix regression introduced by lexer tweak.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    related_to := (
        SELECT I FILTER I.number = '3'
    ),
    tags := ['regression', 'lexer']
};

# NOTE: UPDATE Users for testing the link properties
#
WITH MODULE test
UPDATE User
FILTER User.name = 'Elvis'
SET {
    todo := (SELECT Issue FILTER Issue.number IN {'1', '2'})
};

WITH MODULE test
UPDATE User
FILTER User.name = 'Yury'
SET {
    todo := (SELECT Issue FILTER Issue.number IN {'3', '4'})
};
