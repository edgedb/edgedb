macro_rules! message_group {
    ($(#[$doc:meta])* $group:ident : $super:ident = [$($message:ty),*]) => {
        paste::paste!(
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[allow(unused)]
        pub enum $group {
            $(
                #[doc = concat!("Matched [`", stringify!($message), "`]")]
                $message
            ),*
        }

        #[allow(unused)]
        pub enum [<$group Builder>]<'a> {
            $(
                $message(builder::$message<'a>)
            ),*
        }

        #[allow(unused)]
        impl [<$group Builder>]<'_> {
            pub fn to_vec(&self) -> Vec<u8> {
                match self {
                    $(
                        Self::$message(message) => message.to_vec(),
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
                    if <$message as $crate::protocol::Enliven>::WithLifetime::is_buffer(buf) {
                        return Some(Self::$message);
                    }
                )*
                None
            }

            pub fn match_message(matcher: &mut impl [<$group Match>], buf: &[u8]) {
                $(
                    if data::$message::is_buffer(buf) {
                        if let Some(mut f) = matcher.[<$message:snake>]() {
                            let message = data::$message::new(buf);
                            f(message);
                            return;
                        }
                    }
                )*
            }
        }
        );
    };
}
pub(crate) use message_group;

/// Peform a match on a message.
///
/// ```rust
/// use pgrust::protocol::*;
/// use pgrust::protocol::messages::*;
///
/// let buf = [b'?', 0, 0, 0, 4];
/// match_message!(Message::new(&buf), Backend {
///     (BackendKeyData as data) => {
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
        $(( $i1:path $(as $i2:ident )?) => $impl:block,)*
        $unknown:ident => $unknown_impl:block $(,)?
    }) => {
        {
            let __message = $buf;
            $(
                if let Some(__tmp) = <$i1>::try_new(&__message) {
                    $(let $i2 = __tmp;)?
                    $impl
                } else
            )*
            {
                let $unknown = __message;
                $unknown_impl
            }
        }
    };
}

#[doc(inline)]
pub use __match_message as match_message;
