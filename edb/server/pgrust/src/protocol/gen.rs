macro_rules! protocol {
    ($(
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
                    struct $name {
                        $(
                            #[$doc] $field : $type $( = $value)?
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
        }
    };

    (__one_struct__
        struct $name:ident{
            $(
                // Type is parenthesized here but (T) is equivalent to T. This
                // allows us to keep it as a token tree and perform matches on
                // it.
                #[$doc:meta] $field:ident : $type:ty $( = $value:literal)?
            ),*
        }
    ) => {
        #[allow(non_snake_case, unused_imports)]
        pub mod $name {
            use $crate::protocol::{Enliven, FieldAccess, FieldTypes};
            use $crate::protocol::meta::*;
            const FIELD_COUNT: usize = [$(stringify!($field)),*].len();

            #[allow(unused_imports)]
            pub mod meta {
                use $crate::protocol::{Enliven, FieldAccess};
                use $crate::protocol::meta::*;

                #[allow(unused)]
                pub struct $name {
                }

                impl <'a> Enliven<'a> for $name {
                    type WithLifetime = super::$name<'a>;
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
                }
    
                $crate::protocol::field_access!{$name}
                $crate::protocol::arrays::array_access!{$name}
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

                // protocol!(__measure__ { $( $field : $type ; )* });

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
}
pub(crate) use protocol;
