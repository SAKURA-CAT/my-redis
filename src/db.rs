use crate::resp::Value;
use std::collections::HashMap;

pub struct Storage {
    data: HashMap<String, String>,
    /// key: String, expire: i64 (timestamp + utc)/ms
    expire: HashMap<String, i64>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: HashMap::new(),
            expire: HashMap::new(),
        }
    }

    pub fn get(&self, key: String) -> Value {
        let key = key.as_str();
        if let Some(expire) = self.expire.get(key) {
            if expire < &chrono::Utc::now().timestamp_millis() {
                return Value::Null;
            };
        };
        match self.data.get(key) {
            Some(v) => Value::BulkString(v.to_string()),
            None => Value::Null,
        }
    }

    pub fn set(&mut self, key: String, value: String, expire: Option<i64>) -> Value {
        self.data.insert(key.clone(), value);
        if let Some(expire) = expire {
            self.expire.insert(key, expire + chrono::Utc::now().timestamp_millis());
        }
        Value::SimpleString("OK".to_string())
    }
}

#[cfg(test)]
mod test_storage {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut storage = Storage::new();
        storage.set("foo".to_string(), "bar".to_string(), None);
        assert_eq!(storage.get("foo".to_string()), Value::BulkString("bar".to_string()));
    }

    #[test]
    fn test_expire() {
        let mut storage = Storage::new();
        storage.set("foo".to_string(), "bar".to_string(), Some(-10));
        assert_eq!(storage.get("foo".to_string()), Value::Null);
        storage.set("foo".to_string(), "bar".to_string(), Some(10));
        assert_eq!(storage.get("foo".to_string()), Value::BulkString("bar".to_string()));
        // sleep 11ms, expire
        std::thread::sleep(std::time::Duration::from_millis(11));
        assert_eq!(storage.get("foo".to_string()), Value::Null);
    }
}
