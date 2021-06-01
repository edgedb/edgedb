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
FOR award in {'1st', '2nd', '3rd'} UNION (
    INSERT Award { name := award }
);

WITH MODULE test
INSERT Card {
    name := 'Imp',
    element := 'Fire',
    cost := 1,
    awards := (SELECT Award FILTER .name = '2nd'),
};

WITH MODULE test
INSERT Card {
    name := 'Dragon',
    element := 'Fire',
    cost := 5,
    awards := (SELECT Award FILTER .name = '1st'),
};

WITH MODULE test
INSERT Card {
    name := 'Bog monster',
    element := 'Water',
    cost := 2
};

WITH MODULE test
INSERT Card {
    name := 'Giant turtle',
    element := 'Water',
    cost := 3
};

WITH MODULE test
INSERT Card {
    name := 'Dwarf',
    element := 'Earth',
    cost := 1
};

WITH MODULE test
INSERT Card {
    name := 'Golem',
    element := 'Earth',
    cost := 3
};

WITH MODULE test
INSERT Card {
    name := 'Sprite',
    element := 'Air',
    cost := 1
};

WITH MODULE test
INSERT Card {
    name := 'Giant eagle',
    element := 'Air',
    cost := 2
};

WITH MODULE test
INSERT SpecialCard {
    name := 'Djinn',
    element := 'Air',
    cost := 4,
    awards := (SELECT Award FILTER .name = '3rd'),
};


# create players & decks
WITH MODULE test
INSERT User {
    name := 'Alice',
    deck := (
        SELECT Card {@count := len(Card.element) - 2}
        FILTER .element IN {'Fire', 'Water'}
    ),
    awards := (SELECT Award FILTER .name IN {'1st', '2nd'}),
    avatar := (
        SELECT Card {@text := 'Best'} FILTER .name = 'Dragon'
    ),
};

WITH MODULE test
INSERT User {
    name := 'Bob',
    deck := (
        SELECT Card {@count := 3} FILTER .element IN {'Earth', 'Water'}
    ),
    awards := (SELECT Award FILTER .name = '3rd'),
};

WITH MODULE test
INSERT User {
    name := 'Carol',
    deck := (
        SELECT Card {@count := 5 - Card.cost} FILTER .element != 'Fire'
    )
};

WITH MODULE test
INSERT User {
    name := 'Dave',
    deck := (
        SELECT Card {@count := 4 IF Card.cost = 1 ELSE 1}
        FILTER .element = 'Air' OR .cost != 1
    )
};

# update friends list
WITH
    MODULE test,
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
    MODULE test,
    U2 := DETACHED User
UPDATE User
FILTER User.name = 'Dave'
SET {
    friends := (
        SELECT U2 FILTER U2.name = 'Bob'
    )
};
