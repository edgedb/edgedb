use super::ParseError;
use serde_derive::Serialize;
use std::net::{IpAddr, Ipv6Addr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Host(pub HostType, pub u16);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum HostType {
    Hostname(String),
    IP(IpAddr, Option<String>),
    Path(String),
    Abstract(String),
}

impl std::fmt::Display for HostType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostType::Hostname(hostname) => write!(f, "{}", hostname),
            HostType::IP(ip, Some(interface)) => write!(f, "{}%{}", ip, interface),
            HostType::IP(ip, None) => {
                write!(f, "{}", ip)
            }
            HostType::Path(path) => write!(f, "{}", path),
            HostType::Abstract(name) => write!(f, "@{}", name),
        }
    }
}

impl HostType {
    pub fn try_from_str(s: &str) -> Result<Self, ParseError> {
        if s.is_empty() {
            return Err(ParseError::InvalidHostname("".to_string()));
        }
        if s.contains('[') || s.contains(']') {
            return Err(ParseError::InvalidHostname(s.to_string()));
        }
        if s.starts_with('/') {
            return Ok(HostType::Path(s.to_string()));
        }
        if let Some(s) = s.strip_prefix('@') {
            return Ok(HostType::Abstract(s.to_string()));
        }
        if s.contains('%') {
            let (ip_str, interface) = s.split_once('%').unwrap();
            if interface.is_empty() {
                return Err(ParseError::InvalidHostname(s.to_string()));
            }
            let ip = ip_str
                .parse::<Ipv6Addr>()
                .map_err(|_| ParseError::InvalidHostname(s.to_string()))?;
            return Ok(HostType::IP(IpAddr::V6(ip), Some(interface.to_string())));
        }
        if let Ok(ip) = s.parse::<IpAddr>() {
            Ok(HostType::IP(ip, None))
        } else {
            if s.contains(':') {
                return Err(ParseError::InvalidHostname(s.to_string()));
            }
            Ok(HostType::Hostname(s.to_string()))
        }
    }
}

impl std::str::FromStr for HostType {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        HostType::try_from_str(s)
    }
}
