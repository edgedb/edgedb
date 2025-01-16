/// Performs a first-pass parse on a struct, filling out some additional
/// metadata that makes the jobs of further macro passes much simpler.
///
/// This macro takes a `next` parameter which allows you to funnel the
/// structured data from the macro into the next macro. The complex parsing
/// happens in here using a "push-down automation" technique.
///
/// The term "push-down automation" here refers to how metadata and parsed
/// information are "pushed down" through the macro’s recursive structure. Each
/// level of the macro adds its own layer of processing and metadata
/// accumulation, eventually leading to the final output.
///
/// The `$crate::struct_elaborate!` macro is a tool designed to perform an initial
/// parsing pass on a Rust `struct`, enriching it with metadata to facilitate
/// further macro processing. It begins by extracting and analyzing the fields
/// of the `struct`, capturing associated metadata such as attributes and types.
/// This macro takes a `next` parameter, which is another macro to be invoked
/// after the current one completes its task, allowing for a seamless chaining
/// of macros where each one builds upon the results of the previous.
///
/// The macro first classifies each field based on its type, distinguishing
/// between fixed-size types (like `u8`, `i16`, and arrays) and variable-sized
/// types. It also tracks whether a field has a default value, ensuring that
/// this information is passed along. To handle repetitive or complex patterns,
/// especially when dealing with type information, the macro utilizes the
/// `paste!` macro for duplication and transformation.
///
/// As it processes each field, the macro recursively calls itself, accumulating
/// metadata and updating the state. This recursive approach is structured into
/// different stages, such as `__builder_type__`, `__builder_value__`, and
/// `__finalize__`, each responsible for handling specific aspects of the
/// parsing process.
///
/// Once all fields have been processed, the macro enters the final stage, where
/// it reconstructs an enriched `struct`-like data blob using the accumulated
/// metadata. It then passes this enriched `struct` to the `next` macro for
/// further processing.
#[doc(hidden)]
#[macro_export]
macro_rules! struct_elaborate {
    (
        $next:ident $( ($($next_args:tt)*) )? =>
        $( #[ $sdoc:meta ] )*
        struct $name:ident $(: $super:ident)? {
            $(
                $( #[ doc = $fdoc:literal ] )* $field:ident :
                    $ty:tt $(< $($generics:ident),+ >)?
                    $( = $value:literal)?
            ),*
            $(,)?
        }
    ) => {
        // paste! is necessary here because it allows us to re-interpret a "ty"
        // as an explicit type pattern below.
        $crate::struct_elaborate!(__builder_type__
            // Pass down a "fixed offset" flag that indicates whether the
            // current field is at a fixed offset. This gets reset to
            // `no_fixed_offset` when we hit a variable-sized field.
            fixed(fixed_offset (0))
            fields($(
                [
                    // Note that we double the type so we can re-use some output
                    // patterns in `__builder_type__`
                    type( $ty $(<$($generics),+>)? )( $ty $(<$($generics),+>)? ),
                    value($($value)?),
                    docs($($fdoc)*),
                    name($field),
                ]
            )*)
            // Accumulator for field data.
            accum()
            // Save the original struct parts so we can build the remainder of
            // the struct at the end.
            original($next $( ($($next_args)*) )? => $(#[$sdoc])* struct $name $(: $super)? {}));
    };

    // End of push-down automation - jumps to `__finalize__`
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields() accum($($faccum:tt)*) original($($original:tt)*)) => {
        $crate::struct_elaborate!(__finalize__ accum($($faccum)*) original($($original)*));
    };

    // Skip __builder_value__ for 'len'
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type(len)(len), value(), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_docs__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+4)) fields([type($crate::meta::Length), size(fixed=fixed), value(auto=auto), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type(len)(len), value($($value:tt)+), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_docs__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+4)) fields([type($crate::meta::Length), size(fixed=fixed), value(value=($($value)*)), $($rest)*] $($frest)*) $($srest)*);
    };
    // Pattern match on known fixed-sized types and mark them as `size(fixed=fixed)`
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type([$elem:ty; $len:literal])($ty:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+std::mem::size_of::<$ty>())) fields([type($crate::meta::FixedArray<$len, $elem>), size(fixed=fixed), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type(u8)($ty:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+std::mem::size_of::<$ty>())) fields([type($ty), size(fixed=fixed), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr)fields([type(i16)($ty:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+std::mem::size_of::<$ty>())) fields([type($ty), size(fixed=fixed), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type(i32)($ty:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+std::mem::size_of::<$ty>())) fields([type($ty), size(fixed=fixed), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type(u32)($ty:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+std::mem::size_of::<$ty>())) fields([type($ty), size(fixed=fixed), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type(u64)($ty:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+std::mem::size_of::<$ty>())) fields([type($ty), size(fixed=fixed), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type(Uuid)($ty:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>$fixed $fixed_expr=>($fixed_expr+std::mem::size_of::<$ty>())) fields([type($ty), size(fixed=fixed), $($rest)*] $($frest)*) $($srest)*);
    };

    // Fallback for other types - variable sized
    (__builder_type__ fixed($fixed:ident $fixed_expr:expr) fields([type($ty:ty)($ty2:ty), $($rest:tt)*] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_value__ fixed($fixed=>no_fixed_offset $fixed_expr=>(0)) fields([type($ty), size(variable=variable), $($rest)*] $($frest)*) $($srest)*);
    };

    // Next, mark the presence or absence of a value
    (__builder_value__ fixed($fixed:ident=>$fixed_new:ident $fixed_expr:expr=>$fixed_expr_new:expr) fields([
        type($ty:ty), size($($size:tt)*), value(), $($rest:tt)*
    ] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_docs__ fixed($fixed=>$fixed_new $fixed_expr=>$fixed_expr_new) fields([type($ty), size($($size)*), value(no_value=no_value), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_value__ fixed($fixed:ident=>$fixed_new:ident $fixed_expr:expr=>$fixed_expr_new:expr) fields([
        type($ty:ty), size($($size:tt)*), value($($value:tt)+), $($rest:tt)*
    ] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder_docs__ fixed($fixed=>$fixed_new $fixed_expr=>$fixed_expr_new) fields([type($ty), size($($size)*), value(value=($($value)*)), $($rest)*] $($frest)*) $($srest)*);
    };

    // Next, handle missing docs
    (__builder_docs__ fixed($fixed:ident=>$fixed_new:ident $fixed_expr:expr=>$fixed_expr_new:expr) fields([
        type($ty:ty), size($($size:tt)*), value($($value:tt)*), docs(), name($field:ident), $($rest:tt)*
    ] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder__ fixed($fixed=>$fixed_new $fixed_expr=>$fixed_expr_new) fields([type($ty), size($($size)*), value($($value)*), docs(concat!("`", stringify!($field), "` field.")), name($field), $($rest)*] $($frest)*) $($srest)*);
    };
    (__builder_docs__ fixed($fixed:ident=>$fixed_new:ident $fixed_expr:expr=>$fixed_expr_new:expr) fields([
        type($ty:ty), size($($size:tt)*), value($($value:tt)*), docs($($fdoc:literal)+), $($rest:tt)*
    ] $($frest:tt)*) $($srest:tt)*) => {
        $crate::struct_elaborate!(__builder__ fixed($fixed=>$fixed_new $fixed_expr=>$fixed_expr_new) fields([type($ty), size($($size)*), value($($value)*), docs(concat!($($fdoc)+)), $($rest)*] $($frest)*) $($srest)*);
    };


    // Push down the field to the accumulator
    (__builder__ fixed($fixed:ident=>$fixed_new:ident $fixed_expr:expr=>$fixed_expr_new:expr) fields([
        type($ty:ty), size($($size:tt)*), value($($value:tt)*), docs($fdoc:expr), name($field:ident), $($rest:tt)*
    ] $($frest:tt)*) accum($($faccum:tt)*) original($($original:tt)*)) => {
        $crate::struct_elaborate!(__builder_type__ fixed($fixed_new $fixed_expr_new) fields($($frest)*) accum(
            $($faccum)*
            {
                name($field),
                type($ty),
                size($($size)*),
                value($($value)*),
                docs($fdoc),
                fixed($fixed=$fixed, $fixed_expr),
            },
        ) original($($original)*));
    };

    // Write the final struct
    (__finalize__ accum($($accum:tt)*) original($next:ident $( ($($next_args:tt)*) )?=> $( #[ $sdoc:meta ] )* struct $name:ident $(: $super:ident)? {})) => {
        $next ! (
            $( $($next_args)* , )?
            struct $name {
                super($($super)?),
                docs($($sdoc),*),
                fields(
                    $($accum)*
                ),
            }
        );
    }
}

/// Generates a protocol definition from a Rust-like DSL.
///
/// ```
/// struct Foo {
///     bar: u8,
///     baz: u16,
/// }
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __protocol {
    ($( $( #[ $sdoc:meta ] )* struct $name:ident $(: $super:ident)? { $($struct:tt)+ } )+) => {
        mod access {
            #![allow(unused)]

            /// This struct is specialized for each type we want to extract data from. We
            /// have to do it this way to work around Rust's lack of const specialization.
            pub struct FieldAccess<T: $crate::Enliven> {
                _phantom_data: std::marker::PhantomData<T>,
            }

            $crate::field_access_copy!{basic $crate::FieldAccess, self::FieldAccess,
                i8, u8, i16, u16, i32, u32, i64, u64, i128, u128,
                $crate::meta::Uuid
            }
            $crate::field_access_copy!{$crate::FieldAccess, self::FieldAccess,
                $crate::meta::ZTString,
                $crate::meta::LString,
                $crate::meta::Rest,
                $crate::meta::Encoded,
                $crate::meta::Length
            }
        }

        $(
            $crate::paste!(
                #[allow(unused_imports)]
                pub(crate) mod [<__ $name:lower>] {
                    use $crate::{meta::*, protocol_builder};
                    use super::meta::*;
                    $crate::struct_elaborate!(protocol_builder(__struct__) => $( #[ $sdoc ] )* struct $name $(: $super)? { $($struct)+ } );
                    $crate::struct_elaborate!(protocol_builder(__meta__) => $( #[ $sdoc ] )* struct $name $(: $super)? { $($struct)+ } );
                    $crate::struct_elaborate!(protocol_builder(__measure__) => $( #[ $sdoc ] )* struct $name $(: $super)? { $($struct)+ } );
                    $crate::struct_elaborate!(protocol_builder(__builder__) => $( #[ $sdoc ] )* struct $name $(: $super)? { $($struct)+ } );
                }
            );
        )+

        pub mod data {
            #![allow(unused_imports)]
            $(
                $crate::paste!(
                    pub use super::[<__ $name:lower>]::$name;
                );
            )+
        }
        pub mod meta {
            #![allow(unused_imports)]
            $(
                $crate::paste!(
                    pub use super::[<__ $name:lower>]::[<$name Meta>] as $name;
                );
            )+

            /// A slice containing the metadata references for all structs in
            /// this definition.
            #[allow(unused)]
            pub const ALL: &'static [&'static dyn $crate::Meta] = &[
                $(
                    &$name {}
                ),*
            ];
        }
        pub mod builder {
            #![allow(unused_imports)]
            $(
                $crate::paste!(
                    pub use super::[<__ $name:lower>]::[<$name Builder>] as $name;
                );
            )+
        }
        pub mod measure {
            #![allow(unused_imports)]
            $(
                $crate::paste!(
                    pub use super::[<__ $name:lower>]::[<$name Measure>] as $name;
                );
            )+
        }
    };
}

#[doc(inline)]
pub use __protocol as protocol;

#[macro_export]
#[doc(hidden)]
macro_rules! r#if {
    (__is_empty__ [] {$($true:tt)*} else {$($false:tt)*}) => {
        $($true)*
    };
    (__is_empty__ [$($x:tt)+] {$($true:tt)*} else {$($false:tt)*}) => {
        $($false)*
    };
    (__has__ [$($x:tt)+] {$($true:tt)*}) => {
        $($true)*
    };
    (__has__ [] {$($true:tt)*}) => {
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! protocol_builder {
    (__struct__, struct $name:ident {
        super($($super:ident)?),
        docs($($sdoc:meta),*),
        fields($({
            name($field:ident),
            type($type:ty),
            size($($size:tt)*),
            value($(value = ($value:expr))? $(no_value = $no_value:ident)? $(auto = $auto:ident)?),
            docs($fdoc:expr),
            fixed($fixed:ident=$fixed2:ident, $fixed_expr:expr),
            $($rest:tt)*
        },)*),
    }) => {
        $crate::paste!(
            /// Our struct we are building.
            type S<'a> = $name<'a>;
            /// The meta-struct for the struct we are building.
            type Meta = [<$name Meta>];
            /// The measurement struct (used for `measure`).
            type M<'a> = [<$name Measure>]<'a>;
            /// The builder struct (used for `to_vec` and other build operations)
            type B<'a> = [<$name Builder>]<'a>;
            /// The fields ordinal enum.
            type F<'a> = [<$name Fields>];

            $( #[$sdoc] )?
            #[doc = concat!("\n\nAvailable fields: \n\n" $(
                , " - [`", stringify!($field), "`](Self::", stringify!($field), "()): ", $fdoc,
                $( "  (value = `", stringify!($value), "`)", )?
                "\n\n"
            )* )]
            #[derive(Copy, Clone)]
            pub struct $name<'a> {
                /// Our zero-copy buffer.
                #[doc(hidden)]
                pub(crate) __buf: &'a [u8],
                /// The calculated field offsets.
                #[doc(hidden)]
                __field_offsets: [usize; Meta::FIELD_COUNT + 1]
            }

            impl PartialEq for $name<'_> {
                fn eq(&self, other: &Self) -> bool {
                    self.__buf.eq(other.__buf)
                }
            }

            impl std::fmt::Debug for $name<'_> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    let mut s = f.debug_struct(stringify!($name));
                    $(
                        s.field(stringify!($field), &self.$field());
                    )*
                    s.finish()
                }
            }

            impl AsRef<[u8]> for $name<'_> {
                fn as_ref(&self) -> &[u8] {
                    self.__buf
                }
            }

            #[allow(unused)]
            impl <'a> S<'a> {
                /// Checks the constant values for this struct to determine whether
                /// this message matches.
                #[inline]
                pub const fn is_buffer(buf: &'a [u8]) -> bool {
                    let mut offset = 0;

                    // NOTE! This only works for fixed-sized fields and assumes
                    // that they all exist before variable-sized fields.

                    $(
                        $(
                            let Ok(val) = super::access::FieldAccess::<$type>::extract(buf.split_at(offset).1) else {
                                return false;
                            };
                            if val as usize != $value as usize { return false; }
                        )?
                        offset += std::mem::size_of::<<$type as $crate::Enliven>::ForBuilder<'static>>();
                    )*

                    true
                }

                $(
                    pub const fn can_cast(parent: &<super::meta::$super as $crate::Enliven>::WithLifetime<'a>) -> bool {
                        Self::is_buffer(parent.__buf)
                    }

                    pub const fn try_new(parent: &<super::meta::$super as $crate::Enliven>::WithLifetime<'a>) -> Option<Self> {
                        if Self::can_cast(parent) {
                            // TODO
                            let Ok(value) = Self::new(parent.__buf) else {
                                panic!();
                            };
                            Some(value)
                        } else {
                            None
                        }
                    }
                )?

                /// Creates a new instance of this struct from a given buffer.
                #[inline]
                pub const fn new(mut buf: &'a [u8]) -> Result<Self, $crate::ParseError> {
                    let mut __field_offsets = [0; Meta::FIELD_COUNT + 1];
                    let mut offset = 0;
                    let mut index = 0;
                    $(
                        __field_offsets[index] = offset;
                        offset += match super::access::FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1) {
                            Ok(n) => n,
                            Err(e) => return Err(e),
                        };
                        index += 1;
                    )*
                    __field_offsets[index] = offset;

                    Ok(Self {
                        __buf: buf,
                        __field_offsets,
                    })
                }

                pub fn to_vec(self) -> Vec<u8> {
                    self.__buf.to_vec()
                }

                $(
                    #[doc = $fdoc]
                    #[allow(unused)]
                    #[inline]
                    pub const fn $field<'s>(&'s self) -> <$type as $crate::Enliven>::WithLifetime<'a> where 's : 'a {
                        // Perform a const buffer extraction operation
                        let offset1 = self.__field_offsets[F::$field as usize];
                        let offset2 = self.__field_offsets[F::$field as usize + 1];
                        let (_, buf) = self.__buf.split_at(offset1);
                        let (buf, _) = buf.split_at(offset2 - offset1);
                        // This will not panic: we've confirmed the validity of the buffer when sizing
                        let Ok(value) = super::access::FieldAccess::<$type>::extract(buf) else {
                            panic!();
                        };
                        value
                    }
                )*
            }
        );
    };

    (__meta__, struct $name:ident {
        super($($super:ident)?),
        docs($($sdoc:meta),*),
        fields($({
            name($field:ident),
            type($type:ty),
            size($($size:tt)*),
            value($(value = ($value:expr))? $(no_value = $no_value:ident)? $(auto = $auto:ident)?),
            docs($fdoc:expr),
            fixed($fixed:ident=$fixed2:ident, $fixed_expr:expr),
            $($rest:tt)*
        },)*),
    }) => {
        $crate::paste!(
            $( #[$sdoc] )?
            #[allow(unused)]
            #[derive(Debug, Default)]
            pub struct [<$name Meta>] {
            }

            #[allow(unused)]
            #[allow(non_camel_case_types)]
            #[derive(Eq, PartialEq)]
            #[repr(u8)]
            enum [<$name Fields>] {
                $(
                    $field,
                )*
            }

            #[allow(unused)]
            impl Meta {
                pub const FIELD_COUNT: usize = [$(stringify!($field)),*].len();
                $($(pub const [<$field:upper _VALUE>]: <$type as $crate::Enliven>::WithLifetime<'static> = super::access::FieldAccess::<$type>::constant($value as usize);)?)*
            }

            impl $crate::Meta for Meta {
                fn name(&self) -> &'static str {
                    stringify!($name)
                }
                fn relations(&self) -> &'static [($crate::MetaRelation, &'static dyn $crate::Meta)] {
                    $crate::r#if!(__is_empty__ [$($super)?] {
                        const RELATIONS: &'static [($crate::MetaRelation, &'static dyn $crate::Meta)] = &[
                            $(
                                ($crate::MetaRelation::Field(stringify!($field)), super::access::FieldAccess::<$type>::meta())
                            ),*
                        ];
                    } else {
                        const RELATIONS: &'static [($crate::MetaRelation, &'static dyn $crate::Meta)] = &[
                            ($crate::MetaRelation::Parent, super::access::FieldAccess::<super::meta::$($super)?>::meta()),
                            $(
                                ($crate::MetaRelation::Field(stringify!($field)), super::access::FieldAccess::<$type>::meta())
                            ),*
                        ];
                    });
                    RELATIONS
                }
            }

            $(
                protocol_builder!(__meta__, $fixed($fixed_expr) $field $type);
            )*

            impl $crate::StructMeta for Meta {
                type Struct<'a> = S<'a>;
                fn new(buf: &[u8]) -> Result<S<'_>, $crate::ParseError> {
                    S::new(buf)
                }
                fn to_vec(s: &Self::Struct<'_>) -> Vec<u8> {
                    s.to_vec()
                }
            }

            impl $crate::Enliven for Meta {
                type WithLifetime<'a> = S<'a>;
                type ForMeasure<'a> = M<'a>;
                type ForBuilder<'a> = B<'a>;
            }

            #[allow(unused)]
            impl super::access::FieldAccess<Meta> {
                #[inline(always)]
                pub const fn name() -> &'static str {
                    stringify!($name)
                }
                #[inline(always)]
                pub const fn meta() -> &'static dyn $crate::Meta {
                    &Meta {}
                }
                #[inline]
                pub const fn size_of_field_at(buf: &[u8]) -> Result<usize, $crate::ParseError> {
                    let mut offset = 0;
                    $(
                        offset += match super::access::FieldAccess::<$type>::size_of_field_at(buf.split_at(offset).1) {
                            Ok(n) => n,
                            Err(e) => return Err(e),
                        };
                    )*
                    Ok(offset)
                }
                #[inline(always)]
                pub const fn extract(buf: &[u8]) -> Result<$name<'_>, $crate::ParseError> {
                    $name::new(buf)
                }
                #[inline(always)]
                pub const fn measure(measure: &M) -> usize {
                    measure.measure()
                }
                #[inline(always)]
                pub fn copy_to_buf(buf: &mut $crate::BufWriter, builder: &B) {
                    builder.copy_to_buf(buf)
                }
            }

            use super::access::FieldAccess as FieldAccess;
            $crate::field_access!{self::FieldAccess, [<$name Meta>]}
            $crate::array_access!{variable, self::FieldAccess, [<$name Meta>]}
        );
    };
    (__meta__, fixed_offset($fixed_expr:expr) $field:ident $crate::meta::Length) => {
        impl $crate::StructLength for Meta {
            fn length_field_of(of: &Self::Struct<'_>) -> usize {
                of.$field()
            }
            fn length_field_offset() -> usize {
                $fixed_expr
            }
        }
    };
    (__meta__, $fixed:ident($fixed_expr:expr) $field:ident $crate::meta::Rest) => {

    };
    (__meta__, $fixed:ident($fixed_expr:expr) $field:ident $any:ty) => {
    };

    (__measure__, struct $name:ident {
        super($($super:ident)?),
        docs($($sdoc:meta),*),
        fields($({
            name($field:ident),
            type($type:ty),
            size( $( fixed=$fixed_marker:ident )? $( variable=$variable_marker:ident )? ),
            value($(value = ($value:expr))? $(no_value = $no_value:ident)? $(auto = $auto:ident)?),
            docs($fdoc:expr),
            $($rest:tt)*
        },)*),
    }) => {
        $crate::paste!(
            $crate::r#if!(__is_empty__ [$($($variable_marker)?)*] {
                $( #[$sdoc] )?
                // No variable-sized fields
                #[derive(Default, Eq, PartialEq)]
                pub struct [<$name Measure>]<'a> {
                    __no_fields_use_default: std::marker::PhantomData<&'a ()>
                }
            } else {
                $( #[$sdoc] )?
                pub struct [<$name Measure>]<'a> {
                    // Because of how macros may expand in the context of struct
                    // fields, we need to do a * repeat, then a ? repeat and
                    // somehow use $variable_marker in the remainder of the
                    // pattern.
                    $($(
                        #[doc = $fdoc]
                        pub $field: $crate::r#if!(__has__ [$variable_marker] {<$type as $crate::Enliven>::ForMeasure<'a>}),
                    )?)*
                }
            });

            impl M<'_> {
                pub const fn measure(&self) -> usize {
                    let mut size = 0;
                    $(
                        $crate::r#if!(__has__ [$($variable_marker)?] { size += super::access::FieldAccess::<$type>::measure(&self.$field); });
                        $crate::r#if!(__has__ [$($fixed_marker)?] { size += std::mem::size_of::<<$type as $crate::Enliven>::ForBuilder<'static>>(); });
                    )*
                    size
                }
            }
        );
    };

    (__builder__, struct $name:ident {
        super($($super:ident)?),
        docs($($sdoc:meta),*),
        fields($({
            name($field:ident),
            type($type:ty),
            size($($size:tt)*),
            value($(value = ($value:expr))? $(no_value = $no_value:ident)? $(auto = $auto:ident)?),
            docs($fdoc:expr),
            $($rest:tt)*
        },)*),
    }) => {
        $crate::paste!(
            $crate::r#if!(__is_empty__ [$($($no_value)?)*] {
                $( #[$sdoc] )?
                // No unfixed-value fields
                #[derive(::derive_more::Debug, Default, Eq, PartialEq)]
                pub struct [<$name Builder>]<'a> {
                    #[debug(skip)]
                    __no_fields_use_default: std::marker::PhantomData<&'a ()>
                }
            } else {
                $( #[$sdoc] )?
                #[derive(Debug, Default, Eq, PartialEq)]
                pub struct [<$name Builder>]<'a> {
                    // Because of how macros may expand in the context of struct
                    // fields, we need to do a * repeat, then a ? repeat and
                    // somehow use $no_value in the remainder of the pattern.
                    $($(
                        #[doc = $fdoc]
                        pub $field: $crate::r#if!(__has__ [$no_value] {<$type as $crate::Enliven>::ForBuilder<'a>}),
                    )?)*
                }
            });

            impl B<'_> {
                #[allow(unused)]
                pub fn copy_to_buf(&self, buf: &mut $crate::BufWriter) {
                    $(
                        $crate::r#if!(__is_empty__ [$($value)?] {
                            $crate::r#if!(__is_empty__ [$($auto)?] {
                               <$type as $crate::FieldAccessArray>::copy_to_buf(buf, &self.$field);
                            } else {
                                let auto_offset = buf.size();
                                <$type as $crate::FieldAccessArray>::copy_to_buf(buf, &0);
                            });
                        } else {
                            <$type as $crate::FieldAccessArray>::copy_to_buf(buf, &($($value)? as usize as _));
                        });
                    )*

                    $(
                        $crate::r#if!(__has__ [$($auto)?] {
                            $crate::FieldAccess::<$crate::meta::Length>::copy_to_buf_rewind(buf, auto_offset, buf.size() - auto_offset);
                        });
                    )*

                }

                /// Convert this builder into a vector of bytes. This is generally
                /// not the most efficient way to perform serialization.
                #[allow(unused)]
                pub fn to_vec(&self) -> Vec<u8> {
                    let mut vec = Vec::with_capacity(256);
                    let mut buf = $crate::BufWriter::new(&mut vec);
                    self.copy_to_buf(&mut buf);
                    match buf.finish() {
                        Ok(size) => {
                            vec.truncate(size);
                            vec
                        },
                        Err(size) => {
                            vec.resize(size, 0);
                            let mut buf = $crate::BufWriter::new(&mut vec);
                            self.copy_to_buf(&mut buf);
                            // Will not fail this second time
                            let size = buf.finish().unwrap();
                            vec.truncate(size);
                            vec
                        }
                    }
                }
            }
        );
    };
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    mod fixed_only {
        crate::protocol!(
            struct FixedOnly {
                a: u8,
            }
        );
    }

    mod fixed_only_value {
        crate::protocol!(struct FixedOnlyValue {
            a: u8 = 1,
        });
    }

    mod mixed {
        crate::protocol!(struct Mixed {
            a: u8 = 1,
            s: ZTString,
        });
    }

    mod docs {
        crate::protocol!(
            /// Docs
            struct Docs {
                /// Docs
                a: u8 = 1,
                /// Docs
                s: ZTString,
            }
        );
    }

    mod length {
        crate::protocol!(
            struct WithLength {
                a: u8,
                l: len,
            }
        );
    }

    mod array {
        crate::protocol!(
            struct StaticArray {
                a: u8,
                l: [u8; 4],
            }
        );
    }

    mod string {
        crate::protocol!(
            struct HasLString {
                s: LString,
            }
        );
    }

    macro_rules! assert_stringify {
        (($($struct:tt)*), ($($expected:tt)*)) => {
            $crate::struct_elaborate!(assert_stringify(__internal__ ($($expected)*)) => $($struct)*);
        };
        (__internal__ ($($expected:tt)*), $($struct:tt)*) => {
            // We don't want whitespace to impact this comparison
            if stringify!($($struct)*).replace(char::is_whitespace, "") != stringify!($($expected)*).replace(char::is_whitespace, "") {
                assert_eq!(stringify!($($struct)*), stringify!($($expected)*));
            }
        };
    }

    #[test]
    fn empty_struct() {
        assert_stringify!((struct Foo {}), (struct Foo { super (), docs(), fields(), }));
    }

    #[test]
    fn fixed_size_fields() {
        assert_stringify!((struct Foo {
                    a: u8,
                    b: u8,
                }), (struct Foo
        {
            super (),
            docs(),
            fields({
                name(a), type (u8), size(fixed = fixed), value(no_value = no_value),
                docs(concat!("`", stringify! (a), "` field.")),
                fixed(fixed_offset = fixed_offset, (0)),
            },
            {
                name(b), type (u8), size(fixed = fixed), value(no_value = no_value),
                docs(concat!("`", stringify! (b), "` field.")),
                fixed(fixed_offset = fixed_offset, ((0) + std::mem::size_of::<u8>())),
            },),
        }));
    }

    #[test]
    fn mixed_fields() {
        assert_stringify!((struct Foo {
                    a: u8,
                    l: len,
                    s: ZTString,
                    c: i16,
                    d: [u8; 4],
                    e: ZTArray<ZTString>,
                }), (struct Foo
        {
            super (),
            docs(),
            fields({
                name(a), type (u8), size(fixed = fixed), value(no_value = no_value),
                docs(concat!("`", stringify! (a), "` field.")),
                fixed(fixed_offset = fixed_offset, (0)),
            },
            {
                name(l), type ($crate::meta::Length), size(fixed = fixed),
                value(auto = auto), docs(concat!("`", stringify! (l), "` field.")),
                fixed(fixed_offset = fixed_offset, ((0) + std::mem::size_of::<u8>())),
            },
            {
                name(s), type (ZTString), size(variable = variable),
                value(no_value = no_value),
                docs(concat!("`", stringify! (s), "` field.")),
                fixed(fixed_offset = fixed_offset, (((0) + std::mem::size_of::<u8>()) + 4)),
            },
            {
                name(c), type (i16), size(fixed = fixed), value(no_value = no_value),
                docs(concat!("`", stringify! (c), "` field.")),
                fixed(no_fixed_offset = no_fixed_offset, (0)),
            },
            {
                name(d), type ($crate::meta::FixedArray<4, u8>), size(fixed = fixed),
                value(no_value = no_value),
                docs(concat!("`", stringify! (d), "` field.")),
                fixed(no_fixed_offset = no_fixed_offset, ((0) + std::mem::size_of::<i16>())),
            },
            {
                name(e), type (ZTArray<ZTString>), size(variable = variable),
                value(no_value = no_value),
                docs(concat!("`", stringify! (e), "` field.")),
                fixed(no_fixed_offset = no_fixed_offset,
                    (((0) + std::mem::size_of::<i16>()) +
                    std::mem::size_of::<[u8; 4]>())),
            },
        ),
        }));
    }
}
