
trait TupleNest {
    type Nested;
}

impl TupleNest for () {
    type Nested = ();
}

trait TupleUnnest {
    type Unnested;
}

macro_rules! tuple_nest {
    () => {};
    ($first:ident $(,$tail:ident)*) => {
        tuple_nest!($($tail),*);

        impl <$first,$($tail),*> TupleNest for ($first,$($tail),*) {
            type Nested = ($first, <($($tail,)*) as TupleNest>::Nested);
        }
    };
}

tuple_nest!(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z);
