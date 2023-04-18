type Test4 { 
    name10 : str | int {
        name11 : str | int ;
        property name12 := @name11 * @name11
    }
};