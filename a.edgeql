reset schema to initial;

start migration to { module default {

    abstract type Removable {
        optional single property removed := EXISTS(
            .<element[IS Tombstone]
        );
    };
    type Topic extending Removable {
        multi link defs := .<topic[IS Definition];
    };
    alias VisibleTopic := (
        SELECT Topic {
            defs := (
                SELECT .<topic[IS Definition] FILTER NOT .removed
            ),
        }
        FILTER NOT .removed
    );
    type Definition extending Removable {
        required link topic -> Topic;
    };
    type Tombstone {
        required link element -> Removable {
            constraint exclusive;
        }
    };
} }; populate migration; commit migration;


start migration to { module default {
    abstract type Removable {
        property removed := EXISTS(.<element[IS Tombstone]);
    };
    type Topic extending Removable {
        multi link defs := .<topic[IS Definition];
    };
    alias VisibleTopic := (
        SELECT Topic {
            defs := (
                SELECT .<topic[IS Definition] FILTER NOT .removed
            ),
        }
        FILTER NOT .removed
    );
    type Definition extending Removable {
        required link topic -> Topic;
    };
    type Tombstone {
        required link element -> Removable {
            constraint exclusive;
        }
    };
} }; populate migration; commit migration;
