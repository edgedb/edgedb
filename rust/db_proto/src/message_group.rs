#[doc(hidden)]
#[macro_export]
macro_rules! __message_group {
    ($(#[$doc:meta])* $group:ident : $super:ident = [$($message:ident),*]) => {
        $crate::paste!(
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[allow(unused)]
        pub enum $group {
            $(
                #[doc = concat!("Matched [`", stringify!($message), "`]")]
                $message
            ),*
        }

        #[derive(Debug)]
        #[allow(unused)]
        pub enum [<$group Builder>]<'a> {
            $(
                $message(builder::$message<'a>)
            ),*
        }

        #[allow(unused)]
        impl [<$group Builder>]<'_> {
            pub fn to_vec(self) -> Vec<u8> {
                match self {
                    $(
                        Self::$message(message) => message.to_vec(),
                    )*
                }
            }

            pub fn copy_to_buf(&self, writer: &mut $crate::BufWriter) {
                match self {
                    $(
                        Self::$message(message) => message.copy_to_buf(writer),
                    )*
                }
            }
        }

        $(
        impl <'a> From<builder::$message<'a>> for [<$group Builder>]<'a> {
            fn from(message: builder::$message<'a>) -> Self {
                Self::$message(message)
            }
        }
        )*

        #[allow(unused)]
        pub trait [<$group Match>] {
            $(
                fn [<$message:snake>]<'a>(&mut self) -> Option<impl FnMut(data::$message<'a>)> {
                    // No implementation by default
                    let mut opt = Some(|_| {});
                    opt.take();
                    opt
                }
            )*
            // fn unknown(&mut self, message: self::struct_defs::Message::Message) {
            //     // No implementation by default
            // }
        }

        #[allow(unused)]
        impl $group {
            pub fn identify(buf: &[u8]) -> Option<Self> {
                $(
                    if <meta::$message as $crate::Enliven>::WithLifetime::is_buffer(buf) {
                        return Some(Self::$message);
                    }
                )*
                None
            }

        }
        );
    };
}

#[doc(inline)]
pub use __message_group as message_group;

/// Perform a match on a message.
///
/// ```rust
/// use db_proto::*;
/// use db_proto::test_protocol::data::*;
///
/// let buf = [b'?', 0, 0, 0, 4];
/// match_message!(Message::new(&buf), Backend {
///     (DataRow as data) => {
///         todo!();
///     },
///     unknown => {
///         eprintln!("Unknown message: {unknown:?}");
///     }
/// });
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __match_message {
    ($buf:expr, $messages:ty {
        $(( $i1:path $(as $i2:ident )?) $(if $cond:expr)? => $impl:block,)*
        $unknown:ident => $unknown_impl:block $(,)?
    }) => {
        'block: {
            let __message: Result<_, $crate::ParseError> = $buf;
            let res = match __message {
                Ok(__message) => {
                    $(
                        if $($cond &&)? <$i1>::is_buffer(&__message.as_ref()) {
                            match(<$i1>::new(&__message.as_ref())) {
                                Ok(__tmp) => {
                                    $(let $i2 = __tmp;)?
                                    #[allow(unreachable_code)]
                                    break 'block ({ $impl })
                                }
                                Err(e) => Err(e)
                            }
                        } else
                    )*
                    {
                        Ok(__message)
                    }
                },
                Err(e) => Err(e)
            };
            {
                let $unknown = res;
                #[allow(unreachable_code)]
                break 'block ({ $unknown_impl })
            }
        }
    };
}

#[doc(inline)]
pub use __match_message as match_message;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_protocol::{builder, data::*};

    #[test]
    fn test_match() {
        let message = builder::Sync::default().to_vec();
        let message = Message::new(&message);
        match_message!(message, Message {
            (DataRow as data_row) => {
                eprintln!("{data_row:?}");
                return;
            },
            unknown => {
                eprintln!("{unknown:?}");
                return;
            }
        });
    }
}
