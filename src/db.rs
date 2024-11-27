use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::time::{self, Duration, Instant};

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
    bg_task_notify: Notify,
}

/// DB state entry.
#[derive(Debug, Default)]
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
            bg_task_notify: Notify::new(),
        });
        // Create a background task to purge expired keys.
        tokio::spawn(purge_expired_keys(shared.clone()));
        Db { shared }
    }

    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        // In addition to reduce the bg task's work, we need to judge this key is the next expiration time.
        let mut notify = false;
        let expires_at = expire.map(|d| {
            let when = Instant::now() + d;
            // If the new key is the next expiration time, notify the bg task.
            // First key or earlier than the current next expiration time.
            notify = state.next_expiration().map(|t| t > when).unwrap_or(true);
            when
        });
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

        // Notify the background task to check the expiration time.
        // Before notifying, we need to drop the lock to avoid deadlock.
        drop(state);

        if notify {
            // Only notify the background task if it needs
            self.shared.bg_task_notify.notify_one();
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        let entry = state.entries.get(key)?;
        Some(entry.data.clone())
    }
}

#[cfg(test)]
mod test_db {
    use crate::db::Db;
    use bytes::Bytes;
    use std::time::Duration;

    #[tokio::test]
    async fn test_set_get() {
        let db = Db::new();
        db.set("key1".to_string(), Bytes::from("value1"), None);
        db.set("key2".to_string(), Bytes::from("value2"), Some(Duration::from_secs(1)));

        assert_eq!(db.get("key1").unwrap(), Bytes::from("value1"));
        assert_eq!(db.get("key2").unwrap(), Bytes::from("value2"));
    }

    #[tokio::test]
    async fn test_expire() {
        let db = Db::new();
        db.set(
            "key1".to_string(),
            Bytes::from("value1"),
            Some(Duration::from_millis(100)),
        );
        db.set(
            "key2".to_string(),
            Bytes::from("value2"),
            Some(Duration::from_millis(200)),
        );

        assert_eq!(db.get("key1").unwrap(), Bytes::from("value1"));
        assert_eq!(db.get("key2").unwrap(), Bytes::from("value2"));

        tokio::time::sleep(Duration::from_millis(110)).await;

        assert_eq!(db.get("key1"), None);
        assert_eq!(db.get("key2").unwrap(), Bytes::from("value2"));

        tokio::time::sleep(Duration::from_millis(110)).await;

        assert_eq!(db.get("key2"), None);
    }
}

impl Shared {
    /// Remove expired keys. And return the next expiration time if any.
    pub(crate) fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        // This is needed to make the borrow checker happy.
        // `state.expirations.iter()` borrows `state` immutably, but `state.entries.remove` borrows `state` mutably.
        // So we need to split the borrow and make sure the mutable borrow is dropped before the immutable borrow.
        let state = &mut *state;
        let when = if let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // No more keys to expire.
                return Some(when);
            }
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
            // Return the next expiration time if any.
            // It's different from the mini-redis, which always returns None.
            state.expirations.iter().next().map(|x| x.0)
        } else {
            None
        };
        when
    }
}

#[cfg(test)]
mod test_shared {
    use crate::db::{Db, Shared};
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::Instant;

    fn roughly_equal(a: Instant, b: Instant) -> bool {
        a <= b + Duration::from_millis(10) && a >= b - Duration::from_millis(10)
    }

    impl Db {
        fn delete(&self, key: &str) -> Option<Bytes> {
            let mut state = self.shared.state.lock().unwrap();
            let entry = state.entries.remove(key)?;
            if let Some(expires_at) = entry.expires_at {
                state.expirations.remove(&(expires_at, key.to_string()));
            }
            Some(entry.data)
        }
    }

    #[tokio::test]
    async fn test_purge_expired_keys() {
        let shared = Arc::new(Shared {
            state: Mutex::new(crate::db::State {
                entries: std::collections::HashMap::new(),
                expirations: std::collections::BTreeSet::new(),
            }),
            bg_task_notify: tokio::sync::Notify::new(),
        });
        let db = Db { shared: shared.clone() };

        // Insert a key that will expire in 1 second.
        let first_when = Duration::from_secs(1);
        let second_when = Duration::from_secs(2);
        db.set("key1".to_string(), Bytes::from("value1"), Some(first_when));
        // Insert a key that will expire in 2 seconds.
        db.set("key2".to_string(), Bytes::from("value2"), Some(second_when));

        // The first key should expire in 1 second.
        assert!(
            roughly_equal(shared.purge_expired_keys().unwrap(), Instant::now() + first_when),
            "first key should expire in 1 second"
        );

        // delete the first key
        db.delete("key1");

        assert!(
            roughly_equal(shared.purge_expired_keys().unwrap(), Instant::now() + second_when),
            "second key should expire in 2 seconds"
        );
        // delete the second key
        db.delete("key2");
        // No more keys to expire.
        assert_eq!(shared.purge_expired_keys(), None);
    }
}

async fn purge_expired_keys(shared: Arc<Shared>) {
    loop {
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires, or notified by someone.
            tokio::select! {
                _ = time::sleep_until(when) => {},
                _ = shared.bg_task_notify.notified() => {}
            }
        } else {
            // Wait until notified by someone.
            shared.bg_task_notify.notified().await;
        }
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations.iter().next().map(|x| x.0)
    }
}

#[cfg(test)]
mod test_state {
    use crate::db::State;
    use std::collections::{BTreeSet, HashMap};
    use tokio::time::Instant;

    #[test]
    fn test_next_expiration() {
        let mut state = State {
            entries: HashMap::new(),
            expirations: BTreeSet::new(),
        };
        assert_eq!(state.next_expiration(), None);

        let now = Instant::now();
        state.expirations.insert((now, "key1".to_string()));
        assert_eq!(state.next_expiration(), Some(now));

        // Only return the earliest expiration time.
        let next_now = Instant::now();
        state.expirations.insert((next_now, "key2".to_string()));
        assert_eq!(state.next_expiration(), Some(now));
    }
}
