use std::{collections::HashMap, time::Instant};

use anyhow::{Ok, Result};

use crate::resp::Value;

#[derive(Debug)]
pub struct Item {
    pub value: String,
    pub created: Instant,
    pub expires: usize,
}

pub struct Storage {
    storage: HashMap<String, Item>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            storage: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: &str, value: &str, expires: usize) -> Value {
        let item = Item {
            value: String::from(value),
            created: Instant::now(),
            expires,
        };

        self.storage.insert(String::from(key), item);
        Value::SimpleString("OK".to_string())
    }

    pub fn get(&self, key: &str) -> Option<&Item> {
        let item = self.storage.get(key)?;

        let is_expired =
            item.expires > 0 && item.created.elapsed().as_millis() > item.expires as u128;

        match is_expired {
            true => None,
            false => Some(item),
        }
    }
}

impl Default for Storage {
    fn default() -> Self {
        Storage::new()
    }
}
