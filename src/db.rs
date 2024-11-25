use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// A wrapper around a `Db` instance.
#[derive(Debug)]
pub(crate) struct DbGuard {
    db: Db,
}

/// The main database struct.
/// It will be distributed to different handlers, so it implements `Clone`.
///
/// Additionally, `Clone` will recursively call the clone method of its sub-properties, but the sub-properties are all `Arc`, so it is safe to clone.
/// And it's shallow clone.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Shared>,
}

/// Create a new `DB` instance. All handlers will share the same instance.
#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    // TODO background task to remove expired entries
}

/// DB state entry.
#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
    /// Tracks key TTLs.
    ///
    /// BTreeSet is a sorted set, so we can get the first element which is the earliest expiration time.
    /// While highly unlikely, it is possible for two keys to have the same expiration time. So we also store the key name.
    expirations: BTreeSet<(Instant, String)>,
}

/// Entry in the key-value store.
#[derive(Debug)]
struct Entry {
    /// Stored data
    data: Bytes,
    /// Instant at which the entry expires and should be removed from the database.
    /// None means it will never expire.
    expires_at: Option<Instant>,
}

impl DbGuard {
    pub(crate) fn new() -> Self {
        DbGuard { db: Db::new() }
    }

    /// Get a reference to the `Db` instance.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Db {
    pub(crate) fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
            }),
        });
        Db { shared }
    }

    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        let expires_at = expire.map(|d| Instant::now() + d);
        // Insert the entry into the `HashMap`.
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // Previous entry existed, remove it from the expiration queue.
        if let Some(prev) = prev {
            if let Some(expires_at) = prev.expires_at {
                state.expirations.remove(&(expires_at, key.clone()));
            }
        }

        if let Some(expires_at) = expires_at {
            state.expirations.insert((expires_at, key));
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        let mut state = self.shared.state.lock().unwrap();
        let entry = state.entries.get(key)?;
        if let Some(expires_at) = entry.expires_at {
            if Instant::now() >= expires_at {
                // Entry has expired, remove it.
                // TODO It will be better to use a background task to remove expired entries.
                state.entries.remove(key);
                state.expirations.remove(&(expires_at, key.to_string()));
                return None;
            }
        }
        Some(entry.data.clone())
    }
}
