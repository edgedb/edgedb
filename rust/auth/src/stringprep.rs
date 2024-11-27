use core::str;
use roaring::RoaringBitmap;
use std::{ops::Range, sync::OnceLock};
use unicode_normalization::UnicodeNormalization;

/// Normalize the password using the SASLprep algorithm from RFC4013.
///
/// # Examples
///
/// ```
/// # use gel_auth::stringprep::*;
/// assert_eq!(sasl_normalize_password_bytes(b"password").as_ref(), b"password");
/// assert_eq!(sasl_normalize_password_bytes("passw\u{00A0}rd".as_bytes()).as_ref(), b"passw rd");
/// assert_eq!(sasl_normalize_password_bytes("pass\u{200B}word".as_bytes()).as_ref(), b"password");
/// // This test case demonstrates that invalid UTF-8 sequences are returned unchanged.
/// // The bytes 0xFF, 0xFE, 0xFD do not form a valid UTF-8 sequence, so the function
/// // should return them as-is without attempting to normalize or modify them.
/// assert_eq!(sasl_normalize_password_bytes(&[0xFF, 0xFE, 0xFD]).as_ref(), &[0xFF, 0xFE, 0xFD]);
/// ```
pub fn sasl_normalize_password_bytes(s: &[u8]) -> Cow<[u8]> {
    if s.is_ascii() {
        Cow::Borrowed(s)
    } else if let Ok(s) = str::from_utf8(s) {
        match sasl_normalize_password(s) {
            Cow::Borrowed(s) => Cow::Borrowed(s.as_bytes()),
            Cow::Owned(s) => Cow::Owned(s.into()),
        }
    } else {
        Cow::Borrowed(s)
    }
}

/// Normalize the password using the SASLprep from RFC4013.
///
/// # Examples
///
/// ```
/// # use gel_auth::stringprep::*;
/// assert_eq!(sasl_normalize_password("password").as_ref(), "password");
/// assert_eq!(sasl_normalize_password("passw\u{00A0}rd").as_ref(), "passw rd");
/// assert_eq!(sasl_normalize_password("pass\u{200B}word").as_ref(), "password");
/// assert_eq!(sasl_normalize_password("パスワード").as_ref(), "パスワード"); // precomposed Japanese
/// assert_eq!(sasl_normalize_password("ﾊﾟｽﾜｰﾄﾞ").as_ref(), "パスワード"); // half-width to full-width katakana
/// assert_eq!(sasl_normalize_password("\u{0061}\u{0308}"), "\u{00E4}"); // a + combining diaeresis -> ä
/// assert_eq!(sasl_normalize_password("\u{00E4}"), "\u{00E4}"); // precomposed ä
/// assert_eq!(sasl_normalize_password("\u{0041}\u{0308}"), "\u{00C4}"); // A + combining diaeresis -> Ä
/// assert_eq!(sasl_normalize_password("\u{00C4}"), "\u{00C4}"); // precomposed Ä
/// assert_eq!(sasl_normalize_password("\u{0627}\u{0644}\u{0639}\u{0631}\u{0628}\u{064A}\u{0629}"), "\u{0627}\u{0644}\u{0639}\u{0631}\u{0628}\u{064A}\u{0629}"); // Arabic (RandALCat)
/// ```
pub fn sasl_normalize_password(s: &str) -> Cow<str> {
    if s.is_ascii() {
        return Cow::Borrowed(s);
    }

    let mut normalized = String::with_capacity(s.len());

    // Step 1 of SASLPrep: Map. Per the algorithm, we map non-ascii space
    // characters to ASCII spaces (\x20 or \u0020, but we will use ' ') and
    // commonly mapped to nothing characters are removed
    // Table C.1.2 -- non-ASCII spaces
    // Table B.1 -- "Commonly mapped to nothing"
    for c in s.chars() {
        if !maps_to_nothing::is_char_included(c as u32) {
            if maps_to_space::is_char_included(c as u32) {
                normalized.push(' ');
            } else {
                normalized.push(c);
            }
        }
    }

    // If at this point the password is empty, PostgreSQL uses the original
    // password
    if normalized.is_empty() {
        return Cow::Borrowed(s);
    }

    // Step 2 of SASLPrep: Normalize. Normalize the password using the
    // Unicode normalization algorithm to NFKC form
    let normalized = normalized.chars().nfkc().collect::<String>();

    // If the password is not empty, PostgreSQL uses the original password
    if normalized.is_empty() {
        return Cow::Borrowed(s);
    }

    // Step 3 of SASLPrep: Prohibited characters. If PostgreSQL detects any
    // of the prohibited characters in SASLPrep, it will use the original
    // password
    // We also include "unassigned code points" in the prohibited character
    // category as PostgreSQL does the same
    if normalized.chars().any(is_saslprep_prohibited) {
        return Cow::Borrowed(s);
    }

    // Step 4 of SASLPrep: Bi-directional characters. PostgreSQL follows the
    // rules for bi-directional characters laid on in RFC3454 Sec. 6 which
    // are:
    // 1. Characters in RFC 3454 Sec 5.8 are prohibited (C.8)
    // 2. If a string contains a RandALCat character, it cannot contain any
    //    LCat character
    // 3. If the string contains any RandALCat character, a RandALCat
    //    character must be the first and last character of the string
    // RandALCat characters are found in table D.1, whereas LCat are in D.2.
    // A RandALCat character is a character with unambiguously right-to-left
    // directionality.
    let first_char = normalized.chars().next().unwrap();
    let last_char = normalized.chars().last().unwrap();

    let contains_rand_al_cat = normalized
        .chars()
        .any(|c| table_d1::is_char_included(c as u32));
    if contains_rand_al_cat {
        let contains_l_cat = normalized
            .chars()
            .any(|c| table_d2::is_char_included(c as u32));
        if !table_d1::is_char_included(first_char as u32)
            || !table_d1::is_char_included(last_char as u32)
            || contains_l_cat
        {
            return Cow::Borrowed(s);
        }
    }

    // return the normalized password
    Cow::Owned(normalized)
}

#[macro_export]
macro_rules! __process_ranges {
    (
        $name:ident =>
        $( ($first:literal, $last:literal) )*
    ) => {
        pub mod $name {
            #[allow(unused)]
            pub const RANGES: [std::ops::Range<u32>; [$($first),*].len()] = [
                $(
                    $first..$last,
                )*
            ];

            #[allow(non_contiguous_range_endpoints)]
            #[allow(unused)]
            pub fn is_char_included(c: u32) -> bool {
                match c {
                $(
                    $first..$last => true,
                )*
                _ => false,
                }
            }
        }
    };
}
use std::borrow::Cow;

pub(crate) use __process_ranges as process_ranges;

use super::stringprep_table::{maps_to_nothing, maps_to_space, not_prohibited, table_d1, table_d2};

fn create_bitmap_from_ranges(ranges: &[Range<u32>]) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    for range in ranges {
        bitmap.insert_range(range.clone());
    }
    bitmap
}

static NOT_PROHIBITED_BITMAP: std::sync::OnceLock<RoaringBitmap> = OnceLock::new();

fn get_not_prohibited_bitmap() -> &'static RoaringBitmap {
    NOT_PROHIBITED_BITMAP.get_or_init(|| create_bitmap_from_ranges(&not_prohibited::RANGES))
}

#[inline(always)]
fn is_saslprep_prohibited(c: char) -> bool {
    !get_not_prohibited_bitmap().contains(c as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prohibited() {
        assert!(is_saslprep_prohibited('\0'));
        assert!(is_saslprep_prohibited('\u{100000}'));
    }

    #[test]
    fn generate_roaring_bitmap() {
        let bitmap = create_bitmap_from_ranges(&not_prohibited::RANGES);

        // You can save the bitmap to a file or use it in other ways
        // For example, to save it to a file:
        // use std::fs::File;
        // use std::io::BufWriter;
        // let file = File::create("saslprep_prohibited.bin").unwrap();
        // let mut writer = BufWriter::new(file);
        // bitmap.serialize_into(&mut writer).unwrap();

        // Print some statistics about the bitmap
        println!("Bitmap cardinality: {}", bitmap.len());
        println!("Bitmap size in bytes: {}", bitmap.serialized_size());
    }
}
