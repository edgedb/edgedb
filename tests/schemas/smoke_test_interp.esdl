type Note {
    required single property name -> str;
    optional single property note -> str;
}
type Person {
    required single property name -> str;
    optional multi property multi_prop -> str;
    multi link notes -> Note {
        property metanote -> str;
    }
    optional single property tag -> str;
}
type Foo {
    required single property val -> str;
    optional single property opt -> int64;
}
type Award {
    name : str;
}
type Card {
    single name : str;
    multi awards : Award;
    element : str;
    cost : int64;
}


type User {
    required name: str {
        delegated constraint exclusive;
    }

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
}

type Publication {
    required title: str;

    multi authors: User {
        list_order: int64;
    };
}