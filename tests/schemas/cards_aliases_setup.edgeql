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


CREATE ALIAS test::AirCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.element = 'Air'
);


CREATE ALIAS test::WaterCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.element = 'Water'
);


CREATE ALIAS test::EarthCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.element = 'Earth'
);


CREATE ALIAS test::FireCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.element = 'Fire'
);


CREATE ALIAS test::AliceCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Alice'
);


CREATE ALIAS test::BobCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Bob'
);


CREATE ALIAS test::CarolCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Carol'
);


CREATE ALIAS test::DaveCard := (
    WITH MODULE test
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Dave'
);


CREATE ALIAS test::AliasedFriends := (
    WITH MODULE test
    SELECT User { my_friends := User.friends, my_name := User.name }
);


CREATE ALIAS test::AwardAlias := (
    test::Award {
        # this should be a single link, because awards are exclusive
        winner := test::Award.<awards[IS test::User]
    }
);

# This expression is unnecessarily deep, but that shouldn't have
# any impact as compared to AwardAlias.
CREATE ALIAS test::AwardAlias2 := (
    WITH MODULE test
    SELECT Award {
        winner := Award.<awards[IS test::User] {
            deck: {
                id
            }
        }
    }
);

# This alias includes ordering
CREATE ALIAS test::UserAlias := (
    WITH MODULE test
    SELECT User {
        deck: {
            id
        } ORDER BY User.deck.cost DESC
          LIMIT 1,
    }
);
