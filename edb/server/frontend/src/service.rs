use pgrust::auth::CredentialData;

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
    /// Branch only.
    Branch(String),
    /// DB only (legacy).
    DB(String),
    /// Branch and DB (advanced).
    BranchDB(String, String),
}

#[derive(thiserror::Error, Debug)]
pub enum IdentityError {
    #[error("No user specified")]
    NoUser,
    #[error("No database specified")]
    NoDb,
}

#[derive(Clone, Debug)]
pub struct ConnectionIdentityBuilder {
    tenant: Arc<Mutex<Option<String>>>,
    db: Arc<Mutex<Option<BranchDB>>>,
    user: Arc<Mutex<Option<String>>>,
}

impl ConnectionIdentityBuilder {
    pub fn new() -> Self {
        Self {
            tenant: Arc::new(Mutex::new(None)),
            db: Arc::new(Mutex::new(None)),
            user: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_tenant(&self, tenant: String) -> &Self {
        *self.tenant.lock().unwrap() = Some(tenant);
        self
    }

    pub fn set_branch(&self, branch: BranchDB) -> &Self {
        *self.db.lock().unwrap() = Some(branch);
        self
    }

    pub fn set_user(&self, user: String) -> &Self {
        *self.user.lock().unwrap() = Some(user);
        self
    }

    /// Create a new, disconnected builder.
    pub fn new_builder(&self) -> Self {
        Self {
            tenant: Arc::new(Mutex::new(self.tenant.lock().unwrap().clone())),
            db: Arc::new(Mutex::new(self.db.lock().unwrap().clone())),
            user: Arc::new(Mutex::new(self.user.lock().unwrap().clone())),
        }
    }

    fn unwrap_or_clone<T: Clone>(arc: Arc<Mutex<T>>) -> T {
        match Arc::try_unwrap(arc) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        }
    }

    pub fn build(self) -> Result<ConnectionIdentity, IdentityError> {
        let tenant = Self::unwrap_or_clone(self.tenant);
        let db = Self::unwrap_or_clone(self.db).ok_or(IdentityError::NoDb)?;
        let user = Self::unwrap_or_clone(self.user).ok_or(IdentityError::NoUser)?;

        Ok(ConnectionIdentity { tenant, db, user })
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionIdentity {
    pub tenant: Option<String>,
    pub db: BranchDB,
    pub user: String,
}

/// Handles incoming connections from the listener which might be streams or HTTP.
pub trait BabelfishService: std::fmt::Debug + Send + Sync + 'static {
    fn lookup_auth(
        &self,
        identity: ConnectionIdentity,
        target: AuthTarget,
    ) -> impl Future<Output = Result<CredentialData, std::io::Error>> + Send + Sync;
    fn accept_stream(
        &self,
        identity: ConnectionIdentity,
        language: StreamLanguage,
        stream: ListenerStream,
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send;
    fn accept_http(
        &self,
        identity: ConnectionIdentity,
        req: hyper::http::Request<hyper::body::Incoming>,
    ) -> impl Future<Output = Result<hyper::http::Response<String>, std::io::Error>>;
}
