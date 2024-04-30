# # I really hope this query could work, but due to multiple bugs, it cannot work at the momoent.

# with n0 := (insert Note {name := "boxing", note := {}}),
#      n1 := (insert Note {name := "unboxing", note := "lolol"}),
#      n2 := (insert Note {name := "dynamic", note := "blarg"}),
#      p0 := (insert Person {name := "Phil Emarg", notes := {n0, n1 {@metanote := "arg!"}}}),
#      p1 := (insert Person {name := "Madeline Hatch", notes:={n1 {@metanote := "sigh"}}}),
#      p2 := (insert Person {name := "Emmanuel Villip"}),
#      a_15 := (insert Award {name := "1st"}), 
#      a_e1 := (insert Award {name := "2nd"}),
#      a_ca := (insert Award {name := "3rd"}),
#      c_27 := (insert Card {name := "Imp", element := "Fire", cost := 1, awards := {a_e1}}),
#      c_49 := (insert Card {name := "Dragon", element := "Fire",  cost := 5, awards := {a_15}}),
#      c_80 := (insert Card {name := "Bog monster", element := "Water", cost := 2}),
#      c_d2 := (insert Card {name := "Giant turtle", element := "Water", cost := 3}),
#      c_46 := (insert Card {name := 'Dwarf', element := 'Earth', cost := 1}),
#      c_25 := (insert Card {name := 'Golem', element := 'Earth', cost := 3}),
#      c_bd := (insert Card {name := 'Sprite', element := 'Air', cost := 1}),
#      c_69 := (insert Card {name := 'Giant eagle', element := 'Air', cost := 2}),
#      c_87 := (insert Card {name := 'Djinn', element := 'Air', cost := 4, awards := {a_ca}}),
#      u_3e := (insert User {name := "Carol", deck := {c_80 { @count := 3}, 
#             c_d2 {@count := 2}, c_46 {@count := 4}, c_25 {@count := 2},
#             c_bd {@count := 4}, c_69 {@count := 3}, c_87 {@count := 1}
#         }}),
#     u_fc := (insert User {name := "Bob", deck := {
#             c_80 {@count := 3},
#             c_d2 {@count := 3},
#             c_46 {@count := 3},
#             c_25 {@count := 3}
#         }}), 
#     u_56 := (insert User {name := "Dave", deck := {
#            c_49  {@count:= 1},
#            c_80  {@count:= 1},
#            c_d2  {@count:= 1},
#            c_25  {@count:= 1},
#            c_bd  {@count:= 4},
#            c_69  {@count:= 1},
#            c_87  {@count:= 1}
#         }, friends := {u_fc}, avatar := c_87 {@text := "Wow"}}),
#     u_f3 := (insert User {name := "Alice", deck := {
#             c_27 {@count:= 2},
#             c_49 {@count:= 2},
#             c_80 {@count:= 3},
#             c_d2 {@count:= 3}
#         }, friends := {
#             u_fc {@nickname := "Swampy"},
#             u_3e {@nickname := "Firefighter"},
#             u_56 {@nickname := "Grumpy"}
#         }, awards := {a_15, a_31}, 
#             avatar := {c_49 {@text := "Best"}}
#         }),
    

# select 0;


# COPY of CARDS_SETUP


insert Note {name := "boxing", note := {}};
insert Note {name := "unboxing", note := "lolol"};
insert Note {name := "dynamic", note := "blarg"};
# This obviously should work but it doesn't
# insert Person {name := "Phil Emarg", notes := {(select Note filter .name = "boxing"), 
#                                                 (select Note {@metanote := "arg!"} filter .name = "unboxing")}};
insert Person {name := "Phil Emarg", notes := (select Note {@metanote := <str>{} if .name != "unboxing" else "arg!"}
                                                    filter .name = "boxing" or .name = "unboxing")};
insert Person {name := "Madeline Hatch", notes:=(select Note {@metanote := "sigh"} filter .name = "unboxing")};
insert Person {name := "Emmanuel Villip"};

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
};

INSERT Card {
    name := 'Bog monster',
    element := 'Water',
    cost := 2
};

INSERT Card {
    name := 'Giant turtle',
    element := 'Water',
    cost := 3
};

INSERT Card {
    name := 'Dwarf',
    element := 'Earth',
    cost := 1
};

INSERT Card {
    name := 'Golem',
    element := 'Earth',
    cost := 3
};

INSERT Card {
    name := 'Sprite',
    element := 'Air',
    cost := 1
};

INSERT Card {
    name := 'Giant eagle',
    element := 'Air',
    cost := 2
};

insert Card {
    name := 'Djinn', 
    element := 'Air', 
    cost := 4, 
    awards := (select Award filter .name = "3rd"),
};

# INSERT SpecialCard {
#     name := 'Djinn',
#     element := 'Air',
#     cost := 4,
#     awards := (SELECT Award FILTER .name = '3rd'),
# };


# create players & decks
INSERT User {
    name := 'Alice',
    deck := (
        SELECT Card {@count := len(Card.element) - 2}
        FILTER .element IN {'Fire', 'Water'}
    ),
    awards := (SELECT Award FILTER .name IN {'1st', '2nd'}),
    avatar := (
        SELECT Card {@text := 'Best'} FILTER .name = 'Dragon' LIMIT 1
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

INSERT User {
    name := 'Dave',
    deck := (
        SELECT Card {@count := 4 IF Card.cost = 1 ELSE 1}
        FILTER .element = 'Air' OR .cost != 1
    ),
    avatar := (
        SELECT Card {@text := 'Wow'} FILTER .name = 'Djinn' LIMIT 1
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
