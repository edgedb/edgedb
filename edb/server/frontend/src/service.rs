use crate::stream::ListenerStream;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub enum StreamLanguage {
    Postgres,
    EdgeDB,
    EdgeDBJSON,
    EdgeDBNotebook,
}

pub enum AuthTarget {
    Stream(StreamLanguage),
    HTTP(String),
}

#[derive(Clone, Debug)]
pub enum BranchDB {
    Branch(String),
    DB(String),
}

#[derive(Clone, Debug)]
pub struct ConnectionIdentityBuilder {
    identity: Arc<Mutex<ConnectionIdentity>>,
}

impl ConnectionIdentityBuilder {
    pub fn new() -> Self {
        Self {
            identity: Arc::new(Mutex::new(ConnectionIdentity {
                tenant: None,
                branch: None,
                user: None,
            })),
        }
    }

    pub fn set_tenant(&self, tenant: String) -> &Self {
        let mut identity = self.identity.lock().unwrap();
        identity.tenant = Some(tenant);
        self
    }

    pub fn set_branch(&self, branch: BranchDB) -> &Self {
        let mut identity = self.identity.lock().unwrap();
        identity.branch = Some(branch);
        self
    }

    pub fn set_user(&self, user: String) -> &Self {
        let mut identity = self.identity.lock().unwrap();
        identity.user = Some(user);
        self
    }

    pub fn new_builder(&self) -> Self {
        Self {
            identity: Arc::new(Mutex::new(self.identity.lock().unwrap().clone())),
        }
    }

    pub fn build(self) -> ConnectionIdentity {
        match Arc::try_unwrap(self.identity) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionIdentity {
    tenant: Option<String>,
    branch: Option<BranchDB>,
    user: Option<String>,
}

#[derive(Default)]
pub enum AuthResult {
    #[default]
    Deny,
    Trust,
    MD5(String),
    ScramSHA256(String),
    MTLS,
}

/// Handles incoming connections from the listener which might be streams or HTTP.
pub trait BabelfishService: std::fmt::Debug + Send + Sync + 'static {
    fn lookup_auth(
        &self,
        identity: ConnectionIdentity,
        target: AuthTarget,
    ) -> impl Future<Output = Result<AuthResult, std::io::Error>> + Send + Sync;
    fn accept_stream(
        &self,
        identity: ConnectionIdentity,
        language: StreamLanguage,
        stream: ListenerStream,
    ) -> impl Future<Output = Result<(), std::io::Error>>;
    fn accept_http(
        &self,
        identity: ConnectionIdentity,
        req: hyper::http::Request<hyper::body::Incoming>,
    ) -> impl Future<Output = Result<hyper::http::Response<String>, std::io::Error>>;
}
