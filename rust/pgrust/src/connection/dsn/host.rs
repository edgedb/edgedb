use super::ParseError;
use gel_stream::client::{ResolvedTarget, TargetName};
use serde_derive::Serialize;
use std::net::{IpAddr, Ipv6Addr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Host(pub HostType, pub u16);

impl Host {
    pub fn target_name(&self) -> Result<TargetName, std::io::Error> {
        match &self.0 {
            HostType::Hostname(hostname) => Ok(TargetName::new_tcp((hostname, self.1))),
            HostType::IP(ip, Some(interface)) => Ok(TargetName::new_tcp((
                format!("{}%{}", ip, interface),
                self.1,
            ))),
            HostType::IP(ip, None) => Ok(TargetName::new_tcp((format!("{}", ip), self.1))),
            HostType::Path(path) => {
                TargetName::new_unix_path(format!("{}/.s.PGSQL.{}", path, self.1))
            }
            #[allow(unused)]
            HostType::Abstract(name) => {
                #[cfg(any(target_os = "linux", target_os = "android"))]
                {
                    TargetName::new_unix_domain(format!("{}/.s.PGSQL.{}", name, self.1))
                }
                #[cfg(not(any(target_os = "linux", target_os = "android")))]
                {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "Abstract sockets unsupported on this platform",
                    ))
                }
            }
        }
    }
}

pub trait ToAddrsSyncVec {
    fn to_addrs_sync(&self) -> Vec<(Host, Result<Vec<ResolvedTarget>, std::io::Error>)>;
}

impl ToAddrsSyncVec for Vec<Host> {
    fn to_addrs_sync(&self) -> Vec<(Host, Result<Vec<ResolvedTarget>, std::io::Error>)> {
        let mut result = Vec::with_capacity(self.len());
        for host in self {
            match host.target_name() {
                Ok(target_name) => match target_name.to_addrs_sync() {
                    Ok(addrs) => result.push((host.clone(), Ok(addrs))),
                    Err(err) => result.push((host.clone(), Err(err))),
                },
                Err(err) => {
                    result.push((host.clone(), Err(err)));
                }
            }
        }
        result
    }
}

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
