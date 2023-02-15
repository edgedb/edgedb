select Person filter Person.name = "t1";
# for X in {1,2,3} union X;
# with Y:= {1,2,3} for X in Y union X;
# select (1, 2);
# insert Person {name := "t1"};
# insert Person {name := "t2"};
# insert Person {name := "t3"};
# select {(n1 := (select Person)), (n1 := (select Person))};
# select {(n1 := (select Person)), (n1 := (select Person))}.n1;
# select (l := 1).l;
# insert Person {name := "t1"};
# select true;
# select Person {since := .name};
# select Person.friends@since;

# select {x :=1,y := 2};
# select {x :=1,y := 2};

# with X := Person select X filter .name = "p1" offset 1 limit 1;
# with X := Person, Y := X select X; 


# update Person filter .name = "t2" set {name := "t3"};
# select <Person>{};
# select <std::json>"e";
# select Person {name, last_name} order by .last_name desc then .name then 2 then count(Person.friends);
# select Person {name, last_name} order by .last_name desc then .name;
# select Person {name};
# select Person {name := -2};
# select Person {name : { length, title}};
