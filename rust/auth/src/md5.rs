/// Computes the MD5 password hash used in PostgreSQL authentication.
///
/// This function implements the MD5 password hashing algorithm as specified in the
/// PostgreSQL documentation for MD5 authentication.
///
/// # Algorithm
///
/// 1. Concatenate the password and username.
/// 2. Calculate the MD5 hash of this concatenated string.
/// 3. Concatenate the hexadecimal representation of the hash from step 2 with the salt.
/// 4. Calculate the MD5 hash of the result from step 3.
/// 5. Return the final hash as hex, prefixed with "md5".
///
/// # Example
///
/// ```
/// # use gel_auth::md5::*;
/// let password = "secret";
/// let username = "user";
/// let salt = [0x01, 0x02, 0x03, 0x04];
/// let hash = md5_password(password, username, &salt);
/// assert_eq!(hash, "md5fccef98e4f1cf6cbe96b743fad4e8bd0");
/// ```
pub fn md5_password(password: &str, username: &str, salt: &[u8; 4]) -> String {
    // First MD5 hash of password + username
    let mut hasher = md5::Context::new();
    hasher.consume(password.as_bytes());
    hasher.consume(username.as_bytes());
    let first_hash = hasher.compute();

    // Convert first hash to hex string
    let first_hash_hex = to_hex_string(&first_hash.0);

    // Second MD5 hash of first hash + salt
    let mut hasher = md5::Context::new();
    hasher.consume(first_hash_hex.as_bytes());
    hasher.consume(salt);
    let second_hash = hasher.compute();

    // Combine 'md5' prefix with final hash
    format!("md5{}", to_hex_string(&second_hash.0))
}

/// Converts a byte slice to a hexadecimal string.
fn to_hex_string(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

/// Postgres stores `MD5(username || password)`.
#[derive(Clone, Copy, Debug)]
pub struct StoredHash {
    pub hash: [u8; 16],
}

impl StoredHash {
    pub fn generate(password: &[u8], username: &str) -> Self {
        // First MD5 hash of password + username
        let mut hasher = md5::Context::new();
        hasher.consume(password);
        hasher.consume(username.as_bytes());
        let first_hash = hasher.compute();
        Self { hash: first_hash.0 }
    }

    pub fn matches(&self, client_exchange: &[u8], salt: [u8; 4]) -> bool {
        let this = &self;
        let salt: &[u8; 4] = &salt;
        // Convert first hash to hex string
        let first_hash_hex = to_hex_string(&this.hash);

        // Second MD5 hash of first hash + salt
        let mut hasher = md5::Context::new();
        hasher.consume(first_hash_hex.as_bytes());
        hasher.consume(salt);
        let second_hash = hasher.compute();

        // Convert second hash to hex string
        let second_hash_hex = to_hex_string(&second_hash.0);

        let server_exchange = format!("md5{}", second_hash_hex);
        constant_time_eq::constant_time_eq(client_exchange, server_exchange.as_bytes())
    }
}

impl PartialEq for StoredHash {
    fn eq(&self, other: &Self) -> bool {
        constant_time_eq::constant_time_eq(&self.hash, &other.hash)
    }
}

impl Eq for StoredHash {}
