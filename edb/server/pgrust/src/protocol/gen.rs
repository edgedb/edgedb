macro_rules! protocol {
    ($(
        $( #[ $sdoc:meta ])?
        struct $name:ident {
            $(
                #[ $doc:meta ] $field:ident : $type:ty $( = $value:literal)?
            ),*
            $(,)?
        }
    )+) => {
        // The first phase generates each individual struct and adds the meta
        // and data structs to the `gen` modules.
        #[allow(unused_parens)]
        mod struct_defs {
            $(
                $crate::protocol::gen::protocol!{__one_struct__ 
                    $( #[ $sdoc ] )?
                    struct $name {
                        $(
                            #[$doc]  $field : $type $( = $value)?
                        ),*
                    }
                }
            )*
        }

        pub mod gen {
            pub mod meta {
                $(
                    #[allow(unused_imports)]
                    pub use super::super::struct_defs::$name::meta::$name;
                )*
            }
            pub mod data {
                $(
                    #[allow(unused_imports)]
                    pub use super::super::struct_defs::$name::$name;
                )*
            }
            pub mod measure {
                $(
                    #[allow(unused_imports)]
                    pub use super::super::struct_defs::$name::measure::$name;
                )*
            }
        }
    };

    (__one_struct__
        $( #[ $sdoc:meta ])?
        struct $name:ident{
            $(
                #[$doc:meta] $field:ident : $type:ty $( = $value:literal)?
            ),*
        }
    ) => {
        #[allow(non_snake_case, unused_imports)]
        pub mod $name {
            use $crate::protocol::{Enliven, FieldAccess, FieldTypes, VariableSize};
            use $crate::protocol::meta::*;
            const FIELD_COUNT: usize = [$(stringify!($field)),*].len();

            #[allow(unused_imports)]
            pub mod meta {
                use $crate::protocol::{Enliven, FieldAccess};
                use $crate::protocol::meta::*;
                use $crate::protocol::definition::gen::measure;

                $( #[$sdoc] )?
                #[allow(unused)]
                pub struct $name {
                }

                impl <'a> Enliven<'a> for $name {
                    type WithLifetime = super::$name<'a>;
                    type ForBuilder = measure::$name<'a>;
                }

                impl FieldAccess<$name> {
                    #[inline]
                    pub const fn size_of_field_at(buf: &[u8]) -> usize {
                        let mut offset = 0;
                        $(
                            offset += FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1);
                        )*
                        offset
                    }
                    #[inline(always)]
                    pub const fn extract<'a>(buf: &'a [u8]) -> super::$name<'a> {
                        super::$name::new(buf)
                    }
                    #[inline(always)]
                    pub const fn measure<'a>(measure: &measure::$name) -> usize {
                        measure.measure()
                    }
                }
    
                $crate::protocol::field_access!{$name}
                $crate::protocol::arrays::array_access!{$name}
            }

            pub mod measure {
                use $crate::protocol::meta::*;
                $crate::protocol::gen::protocol!{__measure__ 
                    struct $name {
                        $(
                            #[$doc] $field : $type $( = $value)?
                        ),*
                    }
                }
            }

            impl FieldTypes for meta::$name {
                type FieldTypes = ($(
                    $type
                ),*);
            }

            #[allow(unused)]
            #[allow(non_camel_case_types)]
            #[derive(Eq, PartialEq)]
            #[repr(u8)]
            enum Fields {
                $(
                    $field,
                )*
            }

            $( #[$sdoc] )?
            #[allow(unused)]
            pub struct $name<'a> {
                buf: &'a [u8],
                fields: [usize; FIELD_COUNT + 1]
            }

            impl PartialEq for $name<'_> {
                fn eq(&self, other: &Self) -> bool {
                    self.buf.eq(other.buf)
                }
            }

            impl std::fmt::Debug for $name<'_> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    let mut s = f.debug_struct(stringify!($name));
                    s.field("buf", &self.buf);
                    s.finish()
                }
            }

            #[allow(unused)]
            impl <'a> $name<'a> {
                #[inline]
                pub const fn new(buf: &'a [u8]) -> Self{
                    let mut fields = [0; FIELD_COUNT + 1];
                    let mut offset = 0;
                    let mut index = 0;
                    $(
                        fields[index] = offset;
                        offset += FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1);
                        index += 1;
                    )*
                    fields[index] = offset;
                    
                    Self {
                        buf,
                        fields,
                    }
                }

                pub const fn measure(measure: measure::$name) -> usize {
                    unimplemented!()
                }

                $(
                    #[allow(unused)]
                    #[inline]
                    pub const fn $field<'s>(&'s self) -> <$type as Enliven<'a>>::WithLifetime where 's : 'a {
                        let offset1 = self.fields[Fields::$field as usize];
                        let offset2 = self.fields[Fields::$field as usize + 1];
                        let (_, buf) = self.buf.split_at(offset1);
                        let (buf, _) = buf.split_at(offset2 - offset1);
                        FieldAccess::<$type>::extract(buf)
                    }
                )*
            }
        }
    };

    // Build a push-down automation to regenerate the struct, but omitting all fixed-sized
    // fields. We parse each field one-by-one, emitting the parts of the struct necessary,
    // and then generate the struct all in one go.
    // https://veykril.github.io/tlborm/decl-macros/patterns/push-down-acc.html
    (__measure__ 
        struct $name:ident{
            $(
                #[$doc:meta] $field:ident : $type:ty $( = $value:literal)?
            ),*
        }
    ) => {
        // paste! is necessary here because it allows us to re-interpret a "ty"
        // as a "tt".
        paste::paste!($crate::protocol::gen::protocol!(__measure__ $name 'a [] [0] [
            $(
                [$field, ($type)],
            )*
        ]););
    };

    (__measure__ $name:ident $lt:lifetime [$($accum:tt)*] [$($accum2:tt)*] [[$field:ident, (u8)], $($body:tt)*]) => {
        $crate::protocol::gen::protocol!(__measure__ $name $lt [$($accum)*] [$($accum2)*
            + std::mem::size_of::<u8>()
        ] [$($body)*]);
    };
    (__measure__ $name:ident $lt:lifetime [$($accum:tt)*] [$($accum2:tt)*] [[$field:ident, (i16)], $($body:tt)*]) => {
        $crate::protocol::gen::protocol!(__measure__ $name $lt [$($accum)*] [$($accum2)*
            + std::mem::size_of::<i16>()
        ][$($body)*]);
    };
    (__measure__ $name:ident $lt:lifetime [$($accum:tt)*] [$($accum2:tt)*] [[$field:ident, (i32)], $($body:tt)*]) => {
        $crate::protocol::gen::protocol!(__measure__ $name $lt [$($accum)*] [$($accum2)*
            + std::mem::size_of::<i32>()
        ] [$($body)*]);
    };
    (__measure__ $name:ident $lt:lifetime [$($accum:tt)*] [$($accum2:tt)*] [[$field:ident, ($($type:tt)*)], $($body:tt)*]) => {
        $crate::protocol::gen::protocol!(__measure__ $name $lt [$($accum)* 
            pub $field: <$($type)* as $crate::protocol::Enliven<$lt>>::ForBuilder,
        ] [$($accum2)*
            + $crate::protocol::FieldAccess::<$($type)*>::measure($name.$field)
        ] [$($body)*]);
    };
    // If we end the struct and there are no fields, add a phantom one
    (__measure__ $name:ident $lt:lifetime [] [$($accum2:tt)*] []) => {
        $crate::protocol::gen::protocol!(__measure__ $name $lt [
            phantom: std::marker::PhantomData<&$lt ()>,
        ] [$($accum2)*] []);
    };
    (__measure__ $name:ident $lt:lifetime [$($accum:tt)+] [$($accum2:tt)*] []) => {
        #[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Hash)]
        pub struct $name<$lt> {
            $($accum)*
        }
        impl <$lt> $name<$lt> {
            pub const fn measure(&self) -> usize {
                // Workaround for hygene -- otherwise we need to pass self into the macro
                #[allow(unused)]
                let $name = self;
                $($accum2)*
            }
        }
    };
}
pub(crate) use protocol;
