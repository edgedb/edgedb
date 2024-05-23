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


abstract type Named {
    required name: str {
        delegated constraint exclusive;
    }
}

type User extending Named {
    multi deck: Card {
        count: int64 {
            default := 1;
        };
        property total_cost := @count * .cost;
    }

    property deck_cost := sum(.deck.cost);

    multi friends: User {
        nickname: str;
        # how the friend responded to requests for a favor
        #favor: array<bool>
    }

    multi awards: Award {
        constraint exclusive;
    }

    avatar: Card {
        text: str;
        property tag := .name ++ (("-" ++ @text) ?? "");
    }
    constraint exclusive on (.avatar);
}

type Bot extending User;

type Card extending Named {
    required element: str;
    required cost: int64;
    multi owners := .<deck[IS User];
    # computable property
    elemental_cost := <str>.cost ++ ' ' ++ .element;
    multi awards: Award;
    multi good_awards := (SELECT .awards FILTER .name != '3rd');
    single best_award := (select .awards order by .name limit 1);

    index on (.element);
}

type SpecialCard extending Card;

type Award extending Named {
    link winner := .<awards[is User];
};

alias AirCard := (
    SELECT Card
    FILTER Card.element = 'Air'
);

alias WaterCard := (
    SELECT Card
    FILTER Card.element = 'Water'
);

alias EarthCard := (
    SELECT Card
    FILTER Card.element = 'Earth'
);

alias FireCard := (
    SELECT Card
    FILTER Card.element = 'Fire'
);

alias WaterOrEarthCard := (
    SELECT Card {
        owned_by_alice := EXISTS (SELECT Card.<deck[IS User].name = 'Alice')
    }
    FILTER .element = 'Water' OR .element = 'Earth'
);

alias EarthOrFireCard {
    using (SELECT Card FILTER .element = 'Fire' OR .element = 'Earth')
};

alias AliceCard := (
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Alice'
);

alias BobCard := (
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Bob'
);

alias CarolCard := (
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Carol'
);

alias DaveCard := (
    SELECT Card
    FILTER Card.<deck[IS User].name = 'Dave'
);

alias AliasedFriends := (
    SELECT User { my_friends := User.friends, my_name := User.name }
);

alias AwardAlias := (
    Award {
        # this should be a single link, because awards are exclusive
        winner := Award.<awards[IS User] {
            name_upper := str_upper(.name)
        }
    }
);

# This expression is unnecessarily deep, but that shouldn't have
# any impact as compared to AwardAlias.
alias AwardAlias2 := (
    SELECT Award {
        winner := Award.<awards[IS User] {
            deck: {
                id
            }
        }
    }
);

# This alias includes ordering
alias UserAlias := (
    SELECT User {
        deck: {
            id
        } ORDER BY User.deck.cost DESC
          LIMIT 1,
    }
);

alias SpecialCardAlias := SpecialCard {
    el_cost := (.element, .cost)
};

# This is pulled from test_edgeql_ddl_collection_cleanup_05 to test
# that schemas with tuple aliases like this work properly after the
# fix in #7375.
# This works because one of the patches tests creates the cards database
# before the upgrade and then migrates to it after.
# (See .github/scripts/patches/create-databases.py)
scalar type alias_test_a extending str;
scalar type alias_test_b extending str;
alias alias_test_Bar := (<alias_test_a>"", <alias_test_b>"");
