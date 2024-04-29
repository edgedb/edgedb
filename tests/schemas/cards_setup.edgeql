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

FOR award in {'1st', '2nd', '3rd'} UNION (
    INSERT Award { name := award }
);

INSERT Card {
    name := 'Imp',
    element := 'Fire',
    cost := 1,
    awards := (SELECT Award FILTER .name = '2nd'),
};

INSERT Card {
    name := 'Dragon',
    element := 'Fire',
    cost := 5,
    awards := (SELECT Award FILTER .name IN {'1st', '3rd'}),
    text := '"Watch your back, shoot straight, conserve ammo, and never, ever, cut a deal with a dragon." -Street Proverb',
};

INSERT Card {
    name := 'Bog monster',
    element := 'Water',
    cost := 2,
};

INSERT Card {
    name := 'Giant turtle',
    element := 'Water',
    cost := 3,
    text := '"The world rides through space on the back of a turtle. This is one of the great ancient world myths, found wherever men and turtles were gathered together;"',
};

INSERT Card {
    name := 'Dwarf',
    element := 'Earth',
    cost := 1,
    text := '"The dwarves of yore made mighty spells / While hammers fell like ringing bells"',
};

INSERT Card {
    name := 'Golem',
    element := 'Earth',
    cost := 3,
    # text := '"The only smell Josef could detect arising from the swarthy flesh of the Golem was one too faint to name, acrid and green, that he was only later to identify as the sweet stench, on a summer afternoon in the dog days, of the Moldau."',
    text := '"Every golem in the history of the world, from Rabbi Hanina’s delectable goat to the river-clay Frankenstein of Rabbi Judah Loew ben Bezalel, was summoned into existence through language, through murmuring, recital, and kabbalistic chitchat—was, literally, talked into life."',
};

INSERT Card {
    name := 'Sprite',
    element := 'Air',
    cost := 1,
};

INSERT Card {
    name := 'Giant eagle',
    element := 'Air',
    cost := 2,
    text := '"The North Wind blows, but we shall outfly it"',
};

INSERT SpecialCard {
    name := 'Djinn',
    element := 'Air',
    cost := 4,
    awards := (SELECT Award FILTER .name = '3rd'),
    text := '"Phenomenal cosmic powers! ... Itty bitty living space!"',
};


# create players & decks
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

INSERT User {
    name := 'Bob',
    deck := (
        SELECT Card {@count := 3} FILTER .element IN {'Earth', 'Water'}
    ),
    awards := (SELECT Award FILTER .name = '3rd'),
};

INSERT User {
    name := 'Carol',
    deck := (
        SELECT Card {@count := 5 - Card.cost} FILTER .element != 'Fire'
    )
};

INSERT Bot {
    name := 'Dave',
    deck := (
        SELECT Card {@count := 4 IF Card.cost = 1 ELSE 1}
        FILTER .element = 'Air' OR .cost != 1
    ),
    avatar := (
        SELECT Card {@text := 'Wow'} FILTER .name = 'Djinn'
    ),
};

# update friends list
WITH
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
    U2 := DETACHED User
UPDATE User
FILTER User.name = 'Dave'
SET {
    friends := (
        SELECT U2 FILTER U2.name = 'Bob'
    )
};
