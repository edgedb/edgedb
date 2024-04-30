insert B {name := "b1"};

insert A {name := "a1", b := (select B {@b_lp := "a1_b1_lp"} limit 1)};