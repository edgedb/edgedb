#![allow(private_bounds)]
use super::{Enliven, FieldAccessNonConst};
pub use std::marker::PhantomData;

pub mod meta {
    pub use super::ArrayMeta as Array;
    pub use super::ZTArrayMeta as ZTArray;
}

/// Inflated version of a zero-terminated array with zero-copy iterator access.
pub struct ZTArray<'a, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<T>,
    buf: &'a [u8],
}

pub struct ZTArrayMeta<T> {
    _phantom: PhantomData<T>,
}
impl<'a, T> Enliven<'a> for ZTArrayMeta<T>
where
    T: Enliven<'a>,
    <T as Enliven<'a>>::WithLifetime:
        'a + FieldAccessNonConst<'a, <T as Enliven<'a>>::WithLifetime>,
    <T as Enliven<'a>>::ForMeasure: 'a,
    <T as Enliven<'a>>::ForBuilder: 'a,
{
    type WithLifetime = ZTArray<'a, <T as Enliven<'a>>::WithLifetime>;
    type ForMeasure = &'a [<T as Enliven<'a>>::ForMeasure];
    type ForBuilder = &'a [<T as Enliven<'a>>::ForBuilder];
}

impl<'a, T: FieldAccessNonConst<'a, T> + 'a> ZTArray<'a, T> {
    pub const fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            _phantom: PhantomData,
        }
    }
}

pub struct ZTArrayIter<'a, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<T>,
    buf: &'a [u8],
}

impl<'a, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for ZTArray<'a, T> {
    type Item = T;
    type IntoIter = ZTArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ZTArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
        }
    }
}

impl<'a, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for &ZTArray<'a, T> {
    type Item = T;
    type IntoIter = ZTArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ZTArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
        }
    }
}

impl<'a, T: FieldAccessNonConst<'a, T> + 'a> Iterator for ZTArrayIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf[0] == 0 {
            return None;
        }
        let (value, buf) = self.buf.split_at(T::size_of_field_at(&self.buf));
        self.buf = buf;
        Some(T::extract(value))
    }
}

/// Inflated version of a length-specified array with zero-copy iterator access.
pub struct Array<'a, L: 'static, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<(L, T)>,
    buf: &'a [u8],
    len: u32,
}

pub struct ArrayMeta<L, T> {
    _phantom: PhantomData<(L, T)>,
}

impl<'a, L: 'static, T> Enliven<'a> for ArrayMeta<L, T>
where
    T: Enliven<'a>,
    <T as Enliven<'a>>::WithLifetime:
        'a + FieldAccessNonConst<'a, <T as Enliven<'a>>::WithLifetime>,
    <T as Enliven<'a>>::ForMeasure: 'a,
    <T as Enliven<'a>>::ForBuilder: 'a,
{
    type WithLifetime = Array<'a, L, <T as Enliven<'a>>::WithLifetime>;
    type ForMeasure = &'a [<T as Enliven<'a>>::ForMeasure];
    type ForBuilder = &'a [<T as Enliven<'a>>::ForBuilder];
}

impl<'a, L, T: FieldAccessNonConst<'a, T> + 'a> Array<'a, L, T> {
    pub const fn new(buf: &'a [u8], len: u32) -> Self {
        Self {
            buf,
            _phantom: PhantomData,
            len,
        }
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }
}

pub struct ArrayIter<'a, T: FieldAccessNonConst<'a, T> + 'a> {
    _phantom: PhantomData<T>,
    buf: &'a [u8],
    len: u32,
}

impl<'a, L, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for Array<'a, L, T> {
    type Item = T;
    type IntoIter = ArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
            len: self.len,
        }
    }
}

impl<'a, L, T: FieldAccessNonConst<'a, T> + 'a> IntoIterator for &Array<'a, L, T> {
    type Item = T;
    type IntoIter = ArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
            len: self.len,
        }
    }
}

impl<'a, T: FieldAccessNonConst<'a, T> + 'a> Iterator for ArrayIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        let len = T::size_of_field_at(&self.buf);
        let (value, buf) = self.buf.split_at(len);
        self.buf = buf;
        Some(T::extract(value))
    }
}

/// Definate array accesses for inflated, strongly-typed arrays of both
/// zero-terminated and length-delimited types.
macro_rules! array_access {
    ($ty:ty) => {
        $crate::protocol::arrays::array_access!($ty | u8 i16 i32);
    };
    ($ty:ty | $($len:ty)*) => {
        $(
        #[allow(unused)]
        impl FieldAccess<$crate::protocol::meta::Array<$len, $ty>> {
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                let mut size = std::mem::size_of::<$len>();
                let mut len = FieldAccess::<$len>::extract(buf);
                buf = buf.split_at(size).1;
                loop {
                    if len == 0 {
                        break;
                    }
                    len -= 1;
                    let elem_size = $crate::protocol::FieldAccess::<$ty>::size_of_field_at(buf);
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
                size
            }
            #[inline(always)]
            pub const fn extract<'a>(buf: &'a [u8]) -> $crate::protocol::Array<'a, $len, <$ty as $crate::protocol::Enliven::<'a>>::WithLifetime> {
                let len = FieldAccess::<$len>::extract(buf);
                $crate::protocol::Array::new(buf.split_at(std::mem::size_of::<$len>()).1, len as u32)
            }
            #[inline]
            pub const fn measure<'a>(buffer: &'a[<$ty as Enliven<'a>>::ForMeasure]) -> usize {
                let mut size = std::mem::size_of::<$len>();
                let mut index = 0;
                loop {
                    if index + 1 > buffer.len() {
                        break;
                    }
                    size += FieldAccess::<$ty>::measure(&buffer[index]);
                    index += 1;
                }
                size
            }
            #[inline(always)]
            pub fn copy_to_buf<'a>(buf: &mut $crate::protocol::writer::BufWriter, value: &'a[<$ty as Enliven<'a>>::ForBuilder]) {
                buf.write(&<$len>::to_ne_bytes(value.len() as _));
                for elem in value {
                    FieldAccess::<$ty>::copy_to_buf(buf, elem);
                }
            }

        }
        )*

        #[allow(unused)]
        impl FieldAccess<$crate::protocol::meta::ZTArray<$ty>> {
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                let mut size = 1;
                loop {
                    if buf[0] == 0 {
                        return size;
                    }
                    let elem_size = FieldAccess::<$ty>::size_of_field_at(buf);
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> $crate::protocol::ZTArray<<$ty as Enliven>::WithLifetime> {
                $crate::protocol::ZTArray::new(buf)
            }
            #[inline]
            pub const fn measure<'a>(buffer: &'a[<$ty as Enliven<'a>>::ForMeasure]) -> usize {
                let mut size = 1;
                let mut index = 0;
                loop {
                    if index + 1 > buffer.len() {
                        break;
                    }
                    size += FieldAccess::<$ty>::measure(&buffer[index]);
                    index += 1;
                }
                size
            }
            #[inline(always)]
            pub fn copy_to_buf<'a>(buf: &mut $crate::protocol::writer::BufWriter, value: &[<$ty as Enliven<'a>>::ForBuilder]) {
                for elem in value {
                    FieldAccess::<$ty>::copy_to_buf(buf, elem);
                }
                buf.write_u8(0);
            }
        }
    };
}
pub(crate) use array_access;
