with n0 := (insert Note {name := "boxing", note := {}}),
     n1 := (insert Note {name := "unboxing", note := "lolol"}),
     n2 := (insert Note {name := "dynamic", note := "blarg"}),
     p0 := (insert Person {name := "Phil Emarg", notes := {n0, n1 {@metanote := "arg!"}}}),
     p1 := (insert Person {name := "Madeline Hatch", notes:={n1 {@metanote := "sigh"}}}),
     p2 := (insert Person {name := "Emmanuel Villip"}),
     a_15 := (insert Award {name := "1st"}), 
     a_e1 := (insert Award {name := "2nd"}),
     a_ca := (insert Award {name := "3rd"}),
     c_27 := (insert Card {name := "Imp", element := "Fire", cost := 1, awards := {a_e1}}),
     c_49 := (insert Card {name := "Dragon", element := "Fire",  cost := 5, awards := {a_15}}),
     c_80 := (insert Card {name := "Bog monster", element := "Water", cost := 2}),
     c_d2 := (insert Card {name := "Giant turtle", element := "Water"}),
     c_46 := (insert Card {name := 'Dwarf', element := 'Earth', cost := 1}),
     c_25 := (insert Card {name := 'Golem', element := 'Earth', cost := 3}),
     c_bd := (insert Card {name := 'Sprite', element := 'Air', cost := 1}),
     c_69 := (insert Card {name := 'Giant eagle', element := 'Air', cost := 2}),
     c_87 := (insert Card {name := 'Djinn', element := 'Air', cost := 4, awards := {a_ca}}),
     u_3e := (insert User {name := "Carol", deck := {c_80 { @count := 3}, 
            c_d2 {@count := 2}, c_46 {@count := 4}, c_25 {@count := 2},
            c_bd {@count := 4}, c_69 {@count := 3}, c_87 {@count := 1}
        }}),
    u_fc := (insert User {name := "Bob", deck := {
            c_80 {@count := 3},
            c_d2 {@count := 3},
            c_46 {@count := 3},
            c_25 {@count := 3}
        }}), 
    u_56 := (insert User {name := "Dave", deck := {
           c_49  {@count:= 1},
           c_80  {@count:= 1},
           c_d2  {@count:= 1},
           c_25  {@count:= 1},
           c_bd  {@count:= 4},
           c_69  {@count:= 1},
           c_87  {@count:= 1}
        }, friends := {u_fc}, avatar := c_87 {@text := "Wow"}}),
    u_f3 := (insert User {name := "Alice", deck := {
            c_27 {@count:= 2},
            c_49 {@count:= 2},
            c_80 {@count:= 3},
            c_d2 {@count:= 3}
        }, friends := {
            u_fc {@nickname := "Swampy"},
            u_3e {@nickname := "Firefighter"},
            u_56 {@nickname := "Grumpy"}
        }, awards := {a_15, a_31}, 
            avatar := {c_49 {@text := "Best"}}
        }),
    

select 0;
