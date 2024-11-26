use super::*;
use ::url::Url;
use percent_encoding::{percent_decode_str, utf8_percent_encode};
use std::borrow::Cow;

/// Aggressively encode parameters.
const ENCODING: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.');
const QS_ENCODING: &percent_encoding::AsciiSet = &ENCODING.remove(b'/');

fn maybe_decode(str: Cow<str>) -> Cow<str> {
    if str.contains('%') {
        if let Ok(str) = percent_decode_str(&str).decode_utf8() {
            str.into_owned().into()
        } else {
            str.into_owned().into()
        }
    } else {
        str
    }
}

pub fn parse_postgres_dsn(url_str: &str) -> Result<RawConnectionParameters, ParseError> {
    let url_str = if let Some(url) = url_str.strip_prefix("postgres://") {
        url
    } else if let Some(url) = url_str.strip_prefix("postgresql://") {
        url
    } else {
        return Err(ParseError::InvalidScheme(
            url_str.split(':').next().unwrap_or_default().to_owned(),
        ));
    };

    // Validate percent encoding
    let mut chars = url_str.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex1 = chars.next().ok_or(ParseError::InvalidPercentEncoding)?;
            let hex2 = chars.next().ok_or(ParseError::InvalidPercentEncoding)?;

            if !hex1.is_ascii_hexdigit() || !hex2.is_ascii_hexdigit() {
                return Err(ParseError::InvalidPercentEncoding);
            }

            // Check for %00
            if hex1 == '0' && hex2 == '0' {
                return Err(ParseError::InvalidPercentEncoding);
            }
        }
    }

    // Postgres allows for hostnames surrounded by [] to contain pathnames
    let (authority, path_and_query) = {
        let mut in_brackets = false;
        let mut chars = url_str.char_indices();

        loop {
            if let Some((i, c)) = chars.next() {
                match c {
                    '[' => in_brackets = true,
                    ']' => in_brackets = false,
                    '?' | '/' if !in_brackets => {
                        break url_str.split_at(i);
                    }
                    _ => {}
                }
            } else {
                if in_brackets {
                    return Err(ParseError::InvalidHostname(url_str.to_string()));
                }
                break (url_str, "");
            }
        }
    };

    let (auth, host) = match authority.split_once('@') {
        Some((auth, host)) => (auth, host),
        None => ("", authority),
    };

    let url = Url::parse(&format!("unused://{auth}@host{path_and_query}"))?;

    let mut raw_params = RawConnectionParameters::<'static>::default();

    if host.is_empty() {
        raw_params.host = None;
        raw_params.port = None;
    } else {
        let (hosts, ports) = parse_url_hostlist(maybe_decode(host.into()).split(','))?;
        if !hosts.is_empty() {
            raw_params.host = Some(hosts);
        }
        if !ports.is_empty() {
            raw_params.port = Some(ports);
        }
    };

    raw_params.user = match url.username() {
        "" => None,
        user => {
            let decoded = percent_decode_str(user);
            if let Ok(user) = decoded.decode_utf8() {
                Some(Cow::Owned(user.to_string()))
            } else {
                Some(Cow::Owned(user.to_string()))
            }
        }
    };
    raw_params.password = url
        .password()
        .map(|p| p.into())
        .map(maybe_decode)
        .map(|s| s.into_owned())
        .map(Cow::Owned);
    raw_params.dbname = match url.path() {
        "" | "/" => None,
        path => Some(Cow::Owned(path.trim_start_matches('/').to_string())),
    }
    .map(maybe_decode)
    .map(|s| s.into_owned())
    .map(Cow::Owned);

    // Validate URL query parameters
    let query_str = url.query().unwrap_or("");
    let key_value_pairs = query_str.split('&');

    for pair in key_value_pairs {
        if pair.is_empty() {
            continue;
        }

        if !pair.contains('=') {
            return Err(ParseError::InvalidQueryParameter(pair.to_string()));
        }

        let parts: Vec<&str> = pair.split('=').collect();
        if parts.len() > 2 {
            return Err(ParseError::InvalidQueryParameter(pair.to_string()));
        }

        if parts[0].is_empty() {
            return Err(ParseError::InvalidQueryParameter(pair.to_string()));
        }
    }

    for (mut name, value) in url.query_pairs() {
        // Intentional difference: database is an alias for dbname
        if name == "database" {
            name = Cow::Borrowed("dbname");
        }

        raw_params.set_by_name(&name, value.into_owned().into())?;
    }

    Ok(raw_params)
}

pub fn parse_postgres_dsn_env(
    url_str: &str,
    env: impl EnvVar,
) -> Result<ConnectionParameters, ParseError> {
    let mut raw_params = parse_postgres_dsn(url_str)?;
    raw_params.apply_env(env)?;
    raw_params.try_into()
}

pub(crate) fn params_to_url(params: &RawConnectionParameters) -> String {
    let mut url = String::from("postgresql://");
    let mut params_vec: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();

    // Add user and password if present
    if let Some(user) = &params.user {
        url.extend(utf8_percent_encode(user, ENCODING));
        if let Some(password) = &params.password {
            url.push(':');
            url.extend(utf8_percent_encode(password, ENCODING));
        }
        url.push('@');
    } else if let Some(password) = &params.password {
        url.push(':');
        url.extend(utf8_percent_encode(password, ENCODING));
        url.push('@');
    }

    // Add host and port
    let host_count = params.host.as_ref().map_or(0, |h| h.len());
    let port_count = params.port.as_ref().map_or(0, |p| p.len());

    if host_count <= 1 && port_count <= 1 {
        let mut host_in_qs = false;

        // Add host to authority part
        if let Some(hosts) = &params.host {
            if let Some(Some(host)) = hosts.first() {
                match host {
                    HostType::Hostname(h) => {
                        url.push_str(h);
                    }
                    HostType::IP(ip, Some(h)) => {
                        url.push('[');
                        url.push_str(&ip.to_string());
                        url.push_str("%25");
                        url.push_str(h);
                        url.push(']');
                    }
                    HostType::IP(ip, None) => {
                        url.push('[');
                        url.push_str(&ip.to_string());
                        url.push(']');
                    }
                    _ => {
                        // Unix socket paths go in the params, not in the authority
                        host_in_qs = true;
                        params_vec.push((Cow::Borrowed("host"), Cow::Owned(host.to_string())));
                    }
                }
            } else {
                host_in_qs = true;
                params_vec.push((Cow::Borrowed("host"), Cow::Borrowed("")));
            }
        }

        // Add port to authority part if the host is in the authority part
        if let Some(ports) = &params.port {
            match (host_in_qs, ports.first()) {
                (false, Some(Some(port))) => {
                    url.push(':');
                    url.push_str(&port.to_string());
                }
                (true, Some(Some(port))) => {
                    params_vec.push((Cow::Borrowed("port"), Cow::Owned(port.to_string())));
                }
                (_, Some(None)) => {
                    params_vec.push((Cow::Borrowed("port"), Cow::Borrowed("")));
                }
                (_, None) => {}
            }
        }
    } else {
        // Add hosts to params_vec
        if let Some(hosts) = &params.host {
            let host_str: String = hosts
                .iter()
                .map(|h| h.as_ref().map_or("".to_string(), |h| h.to_string()))
                .collect::<Vec<_>>()
                .join(",");
            params_vec.push((Cow::Borrowed("host"), Cow::Owned(host_str)));
        }

        // Add ports to params_vec
        if let Some(ports) = &params.port {
            let port_str: String = ports
                .iter()
                .map(|&p| p.map_or("".to_string(), |p| p.to_string()))
                .collect::<Vec<_>>()
                .join(",");
            params_vec.push((Cow::Borrowed("port"), Cow::Owned(port_str)));
        }
    }

    // Add database if present
    if let Some(db) = &params.dbname {
        url.push('/');
        url.extend(utf8_percent_encode(db, ENCODING));
    }

    // Add other parameters
    let mut has_query = false;

    if !params_vec.is_empty() {
        has_query = true;
    }

    if !params_vec.is_empty() {
        url.push('?');
        url.push_str(
            &params_vec
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        utf8_percent_encode(k, QS_ENCODING),
                        utf8_percent_encode(v, QS_ENCODING)
                    )
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }

    params.visit_query_only(|key, value| {
        if !has_query {
            url.push('?');
            has_query = true;
        } else {
            url.push('&');
        }
        url.extend(utf8_percent_encode(key, QS_ENCODING));
        url.push('=');
        url.extend(utf8_percent_encode(value, QS_ENCODING));
    });

    url
}

fn parse_port(port: &str) -> Result<Option<u16>, ParseError> {
    if port.is_empty() {
        Ok(None)
    } else if port.contains('%') {
        let decoded = percent_decode_str(port)
            .decode_utf8()
            .map_err(|_| ParseError::InvalidPort(port.to_string()))?;
        Ok(Some(
            decoded
                .parse::<u16>()
                .map_err(|_| ParseError::InvalidPort(port.to_string()))?,
        ))
    } else {
        Ok(Some(
            port.parse::<u16>()
                .map_err(|_| ParseError::InvalidPort(port.to_string()))?,
        ))
    }
}

#[allow(clippy::type_complexity)]
fn parse_url_hostlist<I, S>(
    hostspecs: I,
) -> Result<(Vec<Option<HostType>>, Vec<Option<u16>>), ParseError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    use std::{
        net::{IpAddr, Ipv4Addr},
        str::FromStr,
    };

    let mut hosts = vec![];
    let mut ports = vec![];
    let mut non_empty_host = false;
    let mut non_empty_port = false;

    for hostspec in hostspecs {
        let hostspec = hostspec.as_ref();
        let (host, port) = if let Some(port) = hostspec.strip_prefix(':') {
            (None, parse_port(port)?)
        } else if hostspec.starts_with('/') {
            (Some(HostType::Path(hostspec.to_string())), None)
        } else if hostspec.starts_with('[') {
            let end_bracket = hostspec
                .find(']')
                .ok_or_else(|| ParseError::InvalidHostname(hostspec.to_string()))?;

            let (host_part, port_part) = hostspec.split_at(end_bracket + 1);
            let host = HostType::try_from_str(&host_part[1..end_bracket])?;

            let port = if let Some(stripped) = port_part.strip_prefix(':') {
                parse_port(stripped)?
            } else if !port_part.is_empty() {
                return Err(ParseError::InvalidHostname(hostspec.to_string()));
            } else {
                None
            };
            (Some(host), port)
        } else {
            let parts: Vec<&str> = hostspec.split(':').collect();
            let addr = parts[0].to_string();
            let port = if parts.len() > 1 && !parts[1].is_empty() {
                parse_port(parts[1])?
            } else {
                None
            };

            if let Ok(ip) = Ipv4Addr::from_str(&addr) {
                (Some(HostType::IP(IpAddr::V4(ip), None)), port)
            } else {
                (Some(HostType::Hostname(addr)), port)
            }
        };

        non_empty_host |= host.is_some();
        hosts.push(host);
        non_empty_port |= port.is_some();
        ports.push(port);
    }
    if !non_empty_host && hosts.len() == 1 {
        hosts.clear();
    }
    if !non_empty_port && ports.len() == 1 {
        ports.clear();
    }
    Ok((hosts, ports))
}

#[cfg(test)]
mod tests {
    use super::super::raw_params::SslMode;
    use super::*;
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    #[case(
        &[":1"],
        Ok((vec![], vec![Some(1)]))
    )]
    #[case(
        &[":1", ":2"],
        Ok((vec!["", ""], vec![Some(1), Some(2)]))
    )]
    #[case(
        &["hostname"],
        Ok((vec!["hostname"], vec![]))
    )]
    #[case(
        &["hostname:4321"],
        Ok((vec!["hostname"], vec![Some(4321)]))
    )]
    #[case(
        &["/path"],
        Ok((vec!["/path"], vec![]))
    )]
    #[case(
        &["[2001:db8::1234]", "[::1]"],
        Ok((vec!["2001:db8::1234", "::1"], vec![None, None]))
    )]
    #[case(
        &["[2001:db8::1234%eth0]"],
        Ok((vec!["2001:db8::1234%eth0"], vec![]))
    )]
    #[case(
        &["[::1]z"],
        Err(ParseError::InvalidHostname("[::1]z".to_owned()))
    )]
    fn test_parse_hostlist(
        #[case] input: &[&str],
        #[case] expected: Result<(Vec<&'static str>, Vec<Option<u16>>), ParseError>,
    ) {
        let result = parse_url_hostlist(input);
        let expected_host_types = expected.map(|(hosts, ports)| {
            (
                hosts
                    .into_iter()
                    .map(|h| HostType::try_from_str(h).ok())
                    .collect(),
                ports,
            )
        });
        assert_eq!(expected_host_types, result);
    }

    #[test]
    fn test_parse_dsn() {
        assert_eq!(
            parse_postgres_dsn_env(
                "postgres://",
                [
                    ("PGUSER", "user"),
                    ("PGDATABASE", "testdb"),
                    ("PGPASSWORD", "passw"),
                    ("PGHOST", "host"),
                    ("PGPORT", "123"),
                    ("PGCONNECT_TIMEOUT", "8"),
                ]
                .as_slice()
            )
            .unwrap(),
            ConnectionParameters {
                hosts: vec![Host(HostType::Hostname("host".to_string()), 123)],
                database: "testdb".to_string(),
                user: "user".to_string(),
                password: Password::Specified("passw".to_string()),
                connect_timeout: Some(Duration::from_secs(8)),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );

        assert_eq!(
            parse_postgres_dsn_env("postgres://user:pass@host:1234/database", ()).unwrap(),
            ConnectionParameters {
                hosts: vec![Host(HostType::Hostname("host".to_string()), 1234)],
                database: "database".to_string(),
                user: "user".to_string(),
                password: Password::Specified("pass".to_string()),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );

        assert_eq!(
            parse_postgres_dsn_env("postgresql://user@host1:1111,host2:2222/db", ()).unwrap(),
            ConnectionParameters {
                hosts: vec![
                    Host(HostType::Hostname("host1".to_string()), 1111),
                    Host(HostType::Hostname("host2".to_string()), 2222),
                ],
                database: "db".to_string(),
                user: "user".to_string(),
                password: Password::Unspecified,
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_dsn_with_slashes() {
        assert_eq!(
            parse_postgres_dsn_env(
                r#"postgres://test\\@fgh/test\:db?passfile=/tmp/tmpkrjuaje4"#,
                ()
            )
            .unwrap(),
            ConnectionParameters {
                hosts: vec![Host(HostType::Hostname("fgh".to_string()), 5432)],
                database: r#"test\:db"#.to_string(),
                user: r#"test\\"#.to_string(),
                password: Password::Passfile("/tmp/tmpkrjuaje4".to_string().into()),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_dsn_with_escapes() {
        assert_eq!(
            parse_postgres_dsn_env("postgresql://us%40r:p%40ss@h%40st1,h%40st2:543%33/d%62", ())
                .unwrap(),
            ConnectionParameters {
                hosts: vec![
                    Host(HostType::Hostname("h@st1".to_string()), 5432),
                    Host(HostType::Hostname("h@st2".to_string()), 5433),
                ],
                database: "db".to_string(),
                user: "us@r".to_string(),
                password: Password::Specified("p@ss".to_string()),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_dsn_no_slash() {
        assert_eq!(
            parse_postgres_dsn_env("postgres://user@?port=56226&host=%2Ftmp", ()).unwrap(),
            ConnectionParameters {
                hosts: vec![Host(HostType::Path("/tmp".to_string()), 56226)],
                database: "user".to_string(),
                user: "user".to_string(),
                password: Password::Unspecified,
                ssl: Ssl::Disable,
                ..Default::default()
            }
        );
    }
}
