#![allow(private_bounds)]
use super::{Enliven, FieldAccessArray, FixedSize, Meta, MetaRelation};
use std::fmt::Write;
pub use std::marker::PhantomData;

pub mod meta {
    pub use super::ArrayMeta as Array;
    pub use super::ZTArrayMeta as ZTArray;
}

/// Inflated version of a zero-terminated array with zero-copy iterator access.
pub struct ZTArray<'a, T: FieldAccessArray> {
    _phantom: PhantomData<T>,
    buf: &'a [u8],
}

/// Metaclass for [`ZTArray`].
pub struct ZTArrayMeta<T> {
    pub(crate) _phantom: PhantomData<T>,
}
impl<T: FieldAccessArray> Meta for ZTArrayMeta<T> {
    fn name(&self) -> &'static str {
        "ZTArray"
    }
    fn relations(&self) -> &'static [(MetaRelation, &'static dyn Meta)] {
        &[(MetaRelation::Item, <T as FieldAccessArray>::META)]
    }
}

impl<T> Enliven for ZTArrayMeta<T>
where
    T: FieldAccessArray,
{
    type WithLifetime<'a> = ZTArray<'a, T>;
    type ForMeasure<'a> = &'a [<T as Enliven>::ForMeasure<'a>];
    type ForBuilder<'a> = &'a [<T as Enliven>::ForBuilder<'a>];
}

impl<'a, T: FieldAccessArray> ZTArray<'a, T> {
    pub const fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            _phantom: PhantomData,
        }
    }
}

/// [`ZTArray`] [`Iterator`] for values of type `T`.
pub struct ZTArrayIter<'a, T: FieldAccessArray> {
    _phantom: PhantomData<T>,
    buf: &'a [u8],
}

impl<'a, T> std::fmt::Debug for ZTArray<'a, T>
where
    T: FieldAccessArray,
    <T as Enliven>::WithLifetime<'a>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('[')?;
        for it in self {
            it.fmt(f)?;
            f.write_str(", ")?;
        }
        f.write_char(']')?;
        Ok(())
    }
}

impl<'a, T: FieldAccessArray> IntoIterator for ZTArray<'a, T> {
    type Item = <T as Enliven>::WithLifetime<'a>;
    type IntoIter = ZTArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ZTArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
        }
    }
}

impl<'a, T: FieldAccessArray> IntoIterator for &ZTArray<'a, T> {
    type Item = <T as Enliven>::WithLifetime<'a>;
    type IntoIter = ZTArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ZTArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
        }
    }
}

impl<'a, T: FieldAccessArray> Iterator for ZTArrayIter<'a, T> {
    type Item = <T as Enliven>::WithLifetime<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf[0] == 0 {
            return None;
        }
        let (value, buf) = self.buf.split_at(T::size_of_field_at(self.buf));
        self.buf = buf;
        Some(T::extract(value))
    }
}

/// Inflated version of a length-specified array with zero-copy iterator access.
pub struct Array<'a, L, T: FieldAccessArray> {
    _phantom: PhantomData<(L, T)>,
    buf: &'a [u8],
    len: u32,
}

/// Metaclass for [`Array`].
pub struct ArrayMeta<L, T> {
    pub(crate) _phantom: PhantomData<(L, T)>,
}

impl<L: FieldAccessArray, T: FieldAccessArray> Meta for ArrayMeta<L, T> {
    fn name(&self) -> &'static str {
        "Array"
    }
    fn relations(&self) -> &'static [(MetaRelation, &'static dyn Meta)] {
        &[
            (MetaRelation::Length, L::META),
            (MetaRelation::Item, T::META),
        ]
    }
}

impl<L, T> Enliven for ArrayMeta<L, T>
where
    T: FieldAccessArray,
{
    type WithLifetime<'a> = Array<'a, L, T>;
    type ForMeasure<'a> = &'a [<T as Enliven>::ForMeasure<'a>];
    type ForBuilder<'a> = &'a [<T as Enliven>::ForBuilder<'a>];
}

impl<'a, L, T: FieldAccessArray> Array<'a, L, T> {
    pub const fn new(buf: &'a [u8], len: u32) -> Self {
        Self {
            buf,
            _phantom: PhantomData,
            len,
        }
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<'a, L, T> std::fmt::Debug for Array<'a, L, T>
where
    T: FieldAccessArray,
    <T as Enliven>::WithLifetime<'a>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('[')?;
        for it in self {
            it.fmt(f)?;
            f.write_str(", ")?;
        }
        f.write_char(']')?;
        Ok(())
    }
}

/// [`Array`] [`Iterator`] for values of type `T`.
pub struct ArrayIter<'a, T: FieldAccessArray> {
    _phantom: PhantomData<T>,
    buf: &'a [u8],
    len: u32,
}

impl<'a, L, T: FieldAccessArray> IntoIterator for Array<'a, L, T> {
    type Item = <T as Enliven>::WithLifetime<'a>;
    type IntoIter = ArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
            len: self.len,
        }
    }
}

impl<'a, L, T: FieldAccessArray> IntoIterator for &Array<'a, L, T> {
    type Item = <T as Enliven>::WithLifetime<'a>;
    type IntoIter = ArrayIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        ArrayIter {
            _phantom: PhantomData,
            buf: self.buf,
            len: self.len,
        }
    }
}

impl<'a, T: FieldAccessArray> Iterator for ArrayIter<'a, T> {
    type Item = <T as Enliven>::WithLifetime<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        let len = T::size_of_field_at(self.buf);
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
        impl $crate::protocol::FieldAccess<$crate::protocol::meta::Array<$len, $ty>> {
            pub const fn meta() -> &'static dyn $crate::protocol::Meta {
                &$crate::protocol::meta::Array::<$len, $ty> { _phantom: std::marker::PhantomData }
            }
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                let mut size = std::mem::size_of::<$len>();
                let mut len = $crate::protocol::FieldAccess::<$len>::extract(buf);
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
            pub const fn extract(buf: &[u8]) -> $crate::protocol::Array<'_, $len, $ty> {
                let len = $crate::protocol::FieldAccess::<$len>::extract(buf);
                $crate::protocol::Array::new(buf.split_at(std::mem::size_of::<$len>()).1, len as u32)
            }
            #[inline]
            pub const fn measure<'a>(buffer: &'a[<$ty as $crate::protocol::Enliven>::ForMeasure<'a>]) -> usize {
                let mut size = std::mem::size_of::<$len>();
                let mut index = 0;
                loop {
                    if index + 1 > buffer.len() {
                        break;
                    }
                    let item = &buffer[index];
                    size += $crate::protocol::FieldAccess::<$ty>::measure(item);
                    index += 1;
                }
                size
            }
            #[inline(always)]
            pub fn copy_to_buf<'a>(buf: &mut $crate::protocol::writer::BufWriter, value: &'a[<$ty as $crate::protocol::Enliven>::ForBuilder<'a>]) {
                buf.write(&<$len>::to_be_bytes(value.len() as _));
                for elem in value {
                    $crate::protocol::FieldAccess::<$ty>::copy_to_buf_ref(buf, elem);
                }
            }

        }
        )*

        #[allow(unused)]
        impl $crate::protocol::FieldAccess<$crate::protocol::meta::ZTArray<$ty>> {
            pub const fn meta() -> &'static dyn $crate::protocol::Meta {
                &$crate::protocol::meta::ZTArray::<$ty> { _phantom: std::marker::PhantomData }
            }
            #[inline]
            pub const fn size_of_field_at(mut buf: &[u8]) -> usize {
                let mut size = 1;
                loop {
                    if buf[0] == 0 {
                        return size;
                    }
                    let elem_size = $crate::protocol::FieldAccess::<$ty>::size_of_field_at(buf);
                    buf = buf.split_at(elem_size).1;
                    size += elem_size;
                }
            }
            #[inline(always)]
            pub const fn extract(mut buf: &[u8]) -> $crate::protocol::ZTArray<$ty> {
                $crate::protocol::ZTArray::new(buf)
            }
            #[inline]
            pub const fn measure<'a>(buffer: &'a[<$ty as $crate::protocol::Enliven>::ForMeasure<'a>]) -> usize {
                let mut size = 1;
                let mut index = 0;
                loop {
                    if index + 1 > buffer.len() {
                        break;
                    }
                    let item = &buffer[index];
                    size += $crate::protocol::FieldAccess::<$ty>::measure(item);
                    index += 1;
                }
                size
            }
            #[inline(always)]
            pub fn copy_to_buf(buf: &mut $crate::protocol::writer::BufWriter, value: &[<$ty as $crate::protocol::Enliven>::ForBuilder<'_>]) {
                for elem in value {
                    $crate::protocol::FieldAccess::<$ty>::copy_to_buf_ref(buf, elem);
                }
                buf.write_u8(0);
            }
        }
    };
}
pub(crate) use array_access;

// Arrays of type [`u8`] are special-cased to return a slice of bytes.
impl<T> AsRef<[u8]> for Array<'_, T, u8> {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len as _]
    }
}

// Arrays of fixed-size elements can extract elements in O(1).
impl<'a, L: TryInto<usize>, T: FixedSize + FieldAccessArray> Array<'a, L, T> {
    #[allow(unused)]
    fn get(&self, index: L) -> Option<<T as Enliven>::WithLifetime<'a>> {
        let Ok(index) = index.try_into() else {
            return None;
        };
        let index: usize = index;
        if index >= self.len as _ {
            None
        } else {
            let segment = &self.buf[T::SIZE * index..T::SIZE * (index + 1)];
            Some(T::extract(segment))
        }
    }
}
