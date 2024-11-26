use crate::block::Name;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
};

/// Holds the current drainage and shutdown state for the `Pool`.
#[derive(Default, Debug)]
pub struct Drain {
    drain_all: Cell<usize>,
    drain: RefCell<HashMap<Name, usize>>,
    shutdown: Cell<bool>,
}

impl Drain {
    pub fn shutdown(&self) {
        self.shutdown.set(true)
    }

    pub fn in_shutdown(&self) -> bool {
        self.shutdown.get()
    }

    /// Lock all connections for draining.
    pub fn lock_all<T: AsRef<Drain>>(this: T) -> DrainLock<T> {
        let drain = this.as_ref();
        drain.drain_all.set(drain.drain_all.get() + 1);
        DrainLock {
            db: None,
            has_drain: this,
        }
    }

    // Lock a specific connection for draining.
    pub fn lock<T: AsRef<Drain>>(this: T, db: Name) -> DrainLock<T> {
        {
            let mut drain = this.as_ref().drain.borrow_mut();
            drain.entry(db.clone()).and_modify(|v| *v += 1).or_default();
        }
        DrainLock {
            db: Some(db),
            has_drain: this,
        }
    }

    /// Is this connection draining?
    pub fn is_draining(&self, db: &str) -> bool {
        self.drain_all.get() > 0 || self.drain.borrow().contains_key(db) || self.shutdown.get()
    }

    /// Are any connections draining?
    pub fn are_any_draining(&self) -> bool {
        !self.drain.borrow().is_empty() || self.shutdown.get() || self.drain_all.get() > 0
    }
}

/// Provides a RAII lock for a db- or whole-pool drain operation.
pub struct DrainLock<T: AsRef<Drain>> {
    db: Option<Name>,
    has_drain: T,
}

impl<T: AsRef<Drain>> Drop for DrainLock<T> {
    fn drop(&mut self) {
        if let Some(name) = self.db.take() {
            let mut drain = self.has_drain.as_ref().drain.borrow_mut();
            if let Some(count) = drain.get_mut(&name) {
                if *count >= 1 {
                    *count -= 1;
                } else {
                    drain.remove(&name);
                }
            } else {
                unreachable!()
            }
        } else {
            let this = self.has_drain.as_ref();
            this.drain_all.set(this.drain_all.get() - 1);
        }
    }
}

impl AsRef<Drain> for Drain {
    fn as_ref(&self) -> &Drain {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_lock_all() {
        let drain = Drain::default();
        let l1 = Drain::lock_all(&drain);
        assert!(drain.are_any_draining());
        drop(l1);
        assert!(!drain.are_any_draining());
    }

    #[test]
    fn drain_lock_db_one() {
        let drain = Drain::default();
        assert!(!drain.are_any_draining());
        let l1 = Drain::lock(&drain, "db".into());
        assert!(drain.are_any_draining());
        drop(l1);
        assert!(!drain.are_any_draining());
    }

    #[test]
    fn drain_lock_db_two() {
        let drain = Drain::default();
        let l1 = Drain::lock(&drain, "db".into());
        assert!(drain.are_any_draining());
        let l2 = Drain::lock(&drain, "db".into());
        assert!(drain.are_any_draining());
        drop((l1, l2));
        assert!(!drain.are_any_draining());
    }

    #[test]
    fn drain_lock_db_mixed_one() {
        let drain = Drain::default();
        let l1 = Drain::lock(&drain, "db".into());
        let l2 = Drain::lock(&drain, "db1".into());
        drop((l1, l2));
    }

    #[test]
    fn drain_lock_db_mixed_two() {
        let drain = Drain::default();
        let l1 = Drain::lock(&drain, "db".into());
        let l2 = Drain::lock(&drain, "db1".into());
        let l3 = Drain::lock(&drain, "db1".into());
        drop((l1, l2, l3));
    }
}
