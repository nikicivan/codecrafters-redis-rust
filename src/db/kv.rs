use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct KeyValueStoreIterator<K, V> {
    iter: std::collections::hash_map::IntoIter<K, (V, Option<(Instant, Duration)>)>,
}

impl<K, V> Iterator for KeyValueStoreIterator<K, V> {
    type Item = (K, (V, Option<(Instant, Duration)>));

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Clone, Debug)]
pub struct KeyValueStore<K, V> {
    size: Arc<Mutex<usize>>,
    expire_size: Arc<Mutex<usize>>,
    hash_map: Arc<Mutex<HashMap<K, (V, Option<(Instant, Duration)>)>>>,
}

impl<K, V> KeyValueStore<K, V>
where
    K: Display + Debug + Clone + Eq + std::hash::Hash,
    V: Display + Debug + Clone,
{
    pub fn new() -> Self {
        Self {
            size: Arc::new(Mutex::new(0)),
            expire_size: Arc::new(Mutex::new(0)),
            hash_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_ht_size(&self) -> usize {
        *self.size.lock().await
    }

    pub async fn get_ht_expire_size(&self) -> usize {
        *self.expire_size.lock().await
    }

    pub async fn iter(&self) -> KeyValueStoreIterator<K, V> {
        let map = self.hash_map.lock().await;
        let iter = map.clone().into_iter();
        KeyValueStoreIterator { iter }
    }

    pub async fn insert(&self, k: K, v: V, expiry: Option<Duration>) -> Option<V> {
        let mut guard = self.hash_map.lock().await;
        let val = if expiry.is_some() {
            *self.expire_size.lock().await += 1;
            guard
                .insert(k, (v, Some((Instant::now(), expiry.unwrap()))))
                .map(|v| v.0)
        } else {
            guard.insert(k, (v, None)).map(|v| v.0)
        };
        *self.size.lock().await += 1;
        drop(guard);
        val
    }

    pub async fn get(&self, k: &K) -> Option<V> {
        let now = Instant::now();
        let mut guard = self.hash_map.lock().await;
        let val = if guard.contains_key(&k) {
            let expired = guard.get(&k).and_then(|(_, t)| {
                if t.is_some() {
                    if (now - t.unwrap().0) > t.unwrap().1 {
                        Some(true)
                    } else {
                        Some(false)
                    }
                } else {
                    Some(false)
                }
            });
            if expired.is_some_and(|x| x == true) {
                *self.size.lock().await -= 1;
                *self.expire_size.lock().await -= 1;
                guard.remove(&k);
                None
            } else {
                guard.get(k).and_then(|(val, _)| Some(val)).cloned()
            }
        } else {
            None
        };
        drop(guard);
        val
    }

    pub async fn contains_key(&self, k: &K) -> bool {
        let guard = self.hash_map.lock().await;
        let val = guard.contains_key(k);
        drop(guard);
        val
    }

    pub async fn prune(&self) {
        loop {
            let now = Instant::now();
            // let Self { hash_map, duration } = self;
            let mut guard = self.hash_map.lock().await;
            let keys = guard.keys().cloned().collect::<Vec<K>>();
            for k in keys {
                let expired = guard.get(&k).and_then(|(_, t)| {
                    if t.is_some() {
                        if (now - t.unwrap().0) > t.unwrap().1 {
                            Some(true)
                        } else {
                            Some(false)
                        }
                    } else {
                        Some(false)
                    }
                });

                if expired.is_some_and(|x| x == true) {
                    guard.remove(&k);
                }
            }
            drop(guard);
        }
    }
}
