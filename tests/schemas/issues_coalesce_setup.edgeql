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

INSERT Priority {
    name := 'High'
};

INSERT Priority {
    name := 'Low'
};

INSERT Status {
    name := 'Open'
};

INSERT Status {
    name := 'Closed'
};


INSERT User {
    name := 'Elvis'
};

INSERT URL {
    name := 'edgedb.com',
    address := 'https://edgedb.com'
};

INSERT File {
    name := 'screenshot.png'
};

INSERT LogEntry {
    owner := (SELECT User FILTER User.name = 'Elvis'),
    spent_time := -1,
    body := 'Dummy'
};

INSERT LogEntry {
    owner := (SELECT User FILTER User.name = 'Elvis'),
    spent_time := 60,
    body := 'Log1'
};

INSERT LogEntry {
    owner := (SELECT User FILTER User.name = 'Elvis'),
    spent_time := 90,
    body := 'Log2'
};

INSERT LogEntry {
    owner := (SELECT User FILTER User.name = 'Elvis'),
    spent_time := 60,
    body := 'Log3'
};

INSERT LogEntry {
    owner := (SELECT User FILTER User.name = 'Elvis'),
    spent_time := 30,
    body := 'Log4'
};

INSERT Issue {
    number := '1',
    name := 'Issue 1',
    body := 'Body 1',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    time_estimate := 60,
    time_spent_log := (SELECT LogEntry FILTER LogEntry.body = 'Log1'),
};

INSERT Issue {
    number := '2',
    name := 'Issue 2',
    body := 'Body 2',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    time_estimate := 90,
    time_spent_log := (SELECT LogEntry FILTER LogEntry.body = 'Log2'),
};

INSERT Issue {
    number := '3',
    name := 'Issue 3',
    body := 'Body 3',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    time_estimate := 90,
    time_spent_log := (
        SELECT LogEntry FILTER LogEntry.body IN {'Log3','Log4'}),
};

INSERT Issue {
    number := '4',
    name := 'Issue 4',
    body := 'Body 4',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Open'),
};

WITH
    I := DETACHED Issue
INSERT Issue {
    number := '5',
    name := 'Issue 5',
    body := 'Body 5',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    related_to := (SELECT I FILTER I.number = '1'),
};

WITH
    I := DETACHED Issue
INSERT Issue {
    number := '6',
    name := 'Issue 6',
    body := 'Body 6',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    related_to := (SELECT I FILTER I.number = '2'),
};
