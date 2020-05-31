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
    required property name -> str {
        delegated constraint exclusive;
    }
}

type User extending Named {
    multi link deck -> Card {
        property count -> int64;
    }

    property deck_cost := sum(.deck.cost);

    multi link friends -> User {
        property nickname -> str;
        # how the friend responded to requests for a favor
        #property favor -> array<bool>
    }

    multi link awards -> Award {
        constraint exclusive;
    }

    link avatar -> Card {
        property text -> str;
    }
}

type Card extending Named {
    required property element -> str;
    required property cost -> int64;
    multi link owners := __source__.<deck[IS User];
    # computable property
    property elemental_cost := <str>.cost ++ ' ' ++ .element;
}

type SpecialCard extending Card;

type Award extending Named {
    link rec := .<awards[IS User]
}


alias AwardAlias := Award {
    recipient := .<awards[IS User]
};


alias WaterOrEarthCard := (
    SELECT Card {
        owned_by_alice := EXISTS (SELECT Card.<deck[IS User].name = 'Alice')
    }
    FILTER .element = 'Water' OR .element = 'Earth'
);


alias EarthOrFireCard {
    using (SELECT Card FILTER .element = 'Fire' OR .element = 'Earth')
};


alias SpecialCardAlias := SpecialCard {
    el_cost := (.element, .cost)
};


type Eert {
    required property val -> str {
        constraint exclusive;
    }

    link parent := .<children[IS Eert];
    multi link children -> Eert {
        constraint exclusive;
    }
}


type Report extending Named {
    property subtitle -> str;

    required link user -> User {
        property note -> str;
    }
}
