use serde_derive::Serialize;
use std::{
    fs::OpenOptions,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use super::{Host, HostType};

#[cfg(windows)]
const PGPASSFILE: &str = "pgpass.conf";
#[cfg(not(windows))]
const PGPASSFILE: &str = ".pgpass";

fn read_password_file(
    hosts: &[Host],
    database: &str,
    user: &str,
    reader: impl Iterator<Item = impl AsRef<str>>,
) -> Option<String> {
    for line in reader {
        let line = line.as_ref().trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut parts = vec![String::new()];
        let mut backslash = false;
        for c in line.chars() {
            if backslash {
                parts.last_mut().unwrap().push(c);
                backslash = false;
                continue;
            }
            if c == '\\' {
                backslash = true;
                continue;
            }
            if c == ':' && parts.len() <= 4 {
                parts.push(String::new());
                continue;
            }
            parts.last_mut().unwrap().push(c);
        }

        if parts.len() == 5 {
            for host in hosts {
                match &host.0 {
                    HostType::Hostname(hostname) => {
                        if parts[0] != "*" && parts[0] != hostname.as_str() {
                            continue;
                        }
                    }
                    HostType::IP(hostname, _) => {
                        if parts[0] != "*" && str::parse(&parts[0]) != Ok(*hostname) {
                            continue;
                        }
                    }
                    HostType::Path(_) | HostType::Abstract(_) => {
                        if parts[0] != "*" && parts[0] != "localhost" {
                            continue;
                        }
                    }
                };
                if parts[1] != "*" && str::parse(&parts[1]) != Ok(host.1) {
                    continue;
                }
                if parts[2] != "*" && parts[2] != database {
                    continue;
                }
                if parts[3] != "*" && parts[3] != user {
                    continue;
                }
                return Some(parts.pop().unwrap());
            }
        }
    }

    None
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Password {
    /// The password is unspecified and should be read from the user's default
    /// passfile if it exists.
    #[default]
    Unspecified,
    /// The password was specified.
    Specified(String),
    /// The passfile is specified.
    Passfile(PathBuf),
}

#[derive(Serialize)]
pub enum PasswordWarning {
    NotFile(PathBuf),
    NotExists(PathBuf),
    NotAccessible(PathBuf),
    Permissions(PathBuf, u32),
}

impl std::fmt::Display for PasswordWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PasswordWarning::NotFile(path) => write!(f, "Password file {path:?} is not a plain file"),
            PasswordWarning::NotExists(path) => write!(f, "Password file {path:?} does not exist"),
            PasswordWarning::NotAccessible(path) => write!(f, "Password file {path:?} is not accessible"),
            PasswordWarning::Permissions(path, mode) => write!(f, "Password file {path:?} has group or world access ({mode:o}); permissions should be u=rw (0600) or less"),
        }
    }
}

impl Password {
    pub fn password(&self) -> Option<&str> {
        match self {
            Password::Specified(password) => Some(password),
            _ => None,
        }
    }
}

impl Password {
    /// Attempt to resolve a password against the given homedir.
    pub fn resolve(
        &mut self,
        home: &Path,
        hosts: &[Host],
        database: &str,
        user: &str,
    ) -> Result<Option<PasswordWarning>, std::io::Error> {
        let passfile = match self {
            Password::Unspecified => {
                let passfile = home.join(PGPASSFILE);
                // Don't warn about implicit missing or inaccessible files
                if !matches!(passfile.try_exists(), Ok(true)) {
                    *self = Password::Unspecified;
                    return Ok(None);
                }
                if !passfile.is_file() {
                    *self = Password::Unspecified;
                    return Ok(None);
                }
                passfile
            }
            Password::Specified(_) => return Ok(None),
            Password::Passfile(passfile) => {
                let passfile = passfile.clone();
                if matches!(passfile.try_exists(), Ok(false)) {
                    *self = Password::Unspecified;
                    return Ok(Some(PasswordWarning::NotExists(passfile)));
                }
                if passfile.exists() && !passfile.is_file() {
                    *self = Password::Unspecified;
                    return Ok(Some(PasswordWarning::NotFile(passfile)));
                }
                passfile
            }
        };

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = match passfile.metadata() {
                Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                    *self = Password::Unspecified;
                    return Ok(Some(PasswordWarning::NotAccessible(passfile)));
                }
                res => res?,
            };
            let permissions = metadata.permissions();
            let mode = permissions.mode();

            if mode & (0o070) != 0 {
                *self = Password::Unspecified;
                return Ok(Some(PasswordWarning::Permissions(passfile, mode)));
            }
        }

        let file = match OpenOptions::new().read(true).open(&passfile) {
            Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                *self = Password::Unspecified;
                return Ok(Some(PasswordWarning::NotAccessible(passfile)));
            }
            res => res?,
        };
        if let Some(password) = read_password_file(
            hosts,
            database,
            user,
            std::io::read_to_string(file)?.split('\n'),
        ) {
            *self = Password::Specified(password);
        } else {
            *self = Password::Unspecified;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_password_file() {
        let input = r#"
abc:*:*:user:password from pgpass for user@abc
localhost:*:*:*:password from pgpass for localhost
cde:5433:*:*:password from pgpass for cde:5433

*:*:*:testuser:password from pgpass for testuser
*:*:testdb:*:password from pgpass for testdb
# comment
*:*:test\:db:test\\:password from pgpass with escapes
        "#
        .trim();

        for (host, database, user, output) in [
            (
                Host(HostType::Hostname("abc".to_owned()), 1234),
                "database",
                "user",
                Some("password from pgpass for user@abc"),
            ),
            (
                Host(HostType::Hostname("localhost".to_owned()), 1234),
                "database",
                "user",
                Some("password from pgpass for localhost"),
            ),
            (
                Host(HostType::Path("/tmp".into()), 1234),
                "database",
                "user",
                Some("password from pgpass for localhost"),
            ),
            (
                Host(HostType::Hostname("hmm".to_owned()), 1234),
                "database",
                "testuser",
                Some("password from pgpass for testuser"),
            ),
            (
                Host(HostType::Hostname("hostname".to_owned()), 1234),
                "test:db",
                r#"test\"#,
                Some("password from pgpass with escapes"),
            ),
            (
                Host(HostType::Hostname("doesntexist".to_owned()), 1234),
                "db",
                "user",
                None,
            ),
        ] {
            assert_eq!(
                read_password_file(&[host], database, user, input.split('\n')),
                output.map(|s| s.to_owned())
            );
        }
    }
}
