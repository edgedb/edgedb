use std::collections::HashSet;
use unicode_normalization::UnicodeNormalization;
use base64::decode;
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub struct PasswordHasher {
    digest: Sha256,
}

impl PasswordHasher {
    pub fn new() -> Self {
        PasswordHasher {
            digest: Sha256::new(),
        }
    }

    pub fn generate_salted_password(&self, password: &str, salt: &str, iterations: u32) -> Vec<u8> {
        // Convert the password to a binary string - UTF8 is safe for SASL
        let p = password.as_bytes();
        // The salt needs to be base64 decoded -- full binary must be used
        let s = decode(salt).expect("Invalid base64 salt");

        // The initial signature is the salt with a terminator of a 32-bit string ending in 1
        let mut ui = HmacSha256::new_varkey(p).expect("HMAC can take key of any size");
        ui.update(&(s.clone()));
        ui.update(&[0, 0, 0, 1]);

        // Grab the initial digest
        let mut u = ui.finalize().into_bytes().to_vec();

        // For X number of iterations, recompute the HMAC signature against the password and the latest iteration of the hash, and XOR it with the previous version
        for _ in 0..(iterations - 1) {
            let mut ui = HmacSha256::new_varkey(p).expect("HMAC can take key of any size");
            ui.update(&u);
            u = self.bytes_xor(&u, &ui.finalize().into_bytes().to_vec());
        }

        u
    }

    fn bytes_xor(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        a.iter().zip(b.iter()).map(|(&x1, &x2)| x1 ^ x2).collect()
    }
}

pub struct PasswordNormalizer {
    saslprep_prohibited: Vec<fn(char) -> bool>,
}

impl PasswordNormalizer {
    pub fn new() -> Self {
        // Initialize prohibited character tables
        let saslprep_prohibited = vec![
            stringprep::in_table_c12,
            stringprep::in_table_b1,
            stringprep::in_table_c21_c22,
            stringprep::in_table_c3,
            stringprep::in_table_c4,
            stringprep::in_table_c5,
            stringprep::in_table_c6,
            stringprep::in_table_c7,
            stringprep::in_table_c8,
            stringprep::in_table_c9,
            stringprep::in_table_a1,
        ];
        PasswordNormalizer {
            saslprep_prohibited,
        }
    }

    pub fn normalize_password(&self, original_password: &str) -> String {
        let mut normalized_password = original_password.to_string();

        // If the original password is an ASCII string or fails to encode as a UTF-8 string, then no further action is needed
        if original_password.is_ascii() {
            return original_password.to_string();
        }

        // Step 1 of SASLPrep: Map
        normalized_password = normalized_password
            .chars()
            .map(|c| if stringprep::in_table_c12(c) { ' ' } else { c })
            .filter(|&c| !stringprep::in_table_b1(c))
            .collect();

        // If at this point the password is empty, PostgreSQL uses the original password
        if normalized_password.is_empty() {
            return original_password.to_string();
        }

        // Step 2 of SASLPrep: Normalize
        normalized_password = normalized_password.nfkc().collect();

        // If the password is not empty, PostgreSQL uses the original password
        if normalized_password.is_empty() {
            return original_password.to_string();
        }

        let normalized_password_chars: Vec<char> = normalized_password.chars().collect();

        // Step 3 of SASLPrep: Prohibited characters
        for c in &normalized_password_chars {
            if self.saslprep_prohibited.iter().any(|&f| f(*c)) {
                return original_password.to_string();
            }
        }

        // Step 4 of SASLPrep: Bi-directional characters
        if normalized_password_chars.iter().any(|&c| stringprep::in_table_d1(c)) {
            // if the first character or the last character are not in D.1, return the original password
            if !(stringprep::in_table_d1(normalized_password_chars[0])
                && stringprep::in_table_d1(normalized_password_chars[normalized_password_chars.len() - 1]))
            {
                return original_password.to_string();
            }

            // if any characters are in D.2, use the original password
            if normalized_password_chars.iter().any(|&c| stringprep::in_table_d2(c)) {
                return original_password.to_string();
            }
        }

        // Return the normalized password
        normalized_password
    }
}
