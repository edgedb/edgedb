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

INSERT User {
    name := 'Yury'
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
    spent_time := 50000,
    body := 'Rewriting everything.'
};

INSERT Issue {
    number := '1',
    name := 'Release EdgeDB',
    body := 'Initial public release of EdgeDB.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    watchers := (SELECT User FILTER User.name = 'Yury'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    time_spent_log := (SELECT LogEntry),
    time_estimate := 3000
};

INSERT Comment {
    body := 'EdgeDB needs to happen soon.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    issue := (SELECT Issue FILTER Issue.number = '1')
};


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
    I := DETACHED Issue
INSERT Issue {
    number := '4',
    name := 'Regression.',
    body := 'Fix regression introduced by lexer tweak.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    related_to := (
        SELECT I FILTER I.number = '3'
    )
};

# NOTE: UPDATE Users for testing the link properties
#
UPDATE User
FILTER User.name = 'Elvis'
SET {
    todo := (SELECT Issue FILTER Issue.number IN {'1', '2'})
};

UPDATE User
FILTER User.name = 'Yury'
SET {
    todo := (SELECT Issue FILTER Issue.number IN {'3', '4'})
};

#
# MODULE cards
#

# create some cards
WITH MODULE cards
INSERT Card {
    name := 'Imp',
    element := 'Fire',
    cost := 1
};

WITH MODULE cards
INSERT Card {
    name := 'Dragon',
    element := 'Fire',
    cost := 5
};

WITH MODULE cards
INSERT Card {
    name := 'Bog monster',
    element := 'Water',
    cost := 2
};

WITH MODULE cards
INSERT Card {
    name := 'Giant turtle',
    element := 'Water',
    cost := 3
};

WITH MODULE cards
INSERT Card {
    name := 'Dwarf',
    element := 'Earth',
    cost := 1
};

WITH MODULE cards
INSERT Card {
    name := 'Golem',
    element := 'Earth',
    cost := 3
};

WITH MODULE cards
INSERT Card {
    name := 'Sprite',
    element := 'Air',
    cost := 1
};

WITH MODULE cards
INSERT Card {
    name := 'Giant eagle',
    element := 'Air',
    cost := 2
};

WITH MODULE cards
INSERT Card {
    name := 'Djinn',
    element := 'Air',
    cost := 4
};

# create players & decks
WITH MODULE cards
INSERT User {
    name := 'Alice',
    deck := (
        SELECT Card {@count := len(Card.element) - 2}
        FILTER .element IN {'Fire', 'Water'}
    )
};

WITH MODULE cards
INSERT User {
    name := 'Bob',
    deck := (
        SELECT Card {@count := 3} FILTER .element IN {'Earth', 'Water'}
    )
};

WITH MODULE cards
INSERT User {
    name := 'Carol',
    deck := (
        SELECT Card {@count := 5 - Card.cost} FILTER .element != 'Fire'
    )
};

WITH MODULE cards
INSERT User {
    name := 'Dave',
    deck := (
        SELECT Card {@count := 4 IF Card.cost = 1 ELSE 1}
        FILTER .element = 'Air' OR .cost != 1
    )
};

# update friends list
WITH
    MODULE cards,
    U2 := DETACHED User
UPDATE User
FILTER User.name = 'Alice'
SET {
    friends := (
        SELECT U2 {
            @nickname :=
                'Swampy'        IF U2.name = 'Bob' ELSE
                'Firefighter'   IF U2.name = 'Carol' ELSE
                'Grumpy'
        } FILTER U2.name IN {'Bob', 'Carol', 'Dave'}
    )
};

WITH
    MODULE cards,
    U2 := DETACHED User
UPDATE User
FILTER User.name = 'Dave'
SET {
    friends := (
        SELECT U2 FILTER U2.name = 'Bob'
    )
};
