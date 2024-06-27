use crate::{data::LogPosition, option::IteratorOptions};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

use super::{Index, IndexIterator};

// 跳表索引
pub struct SkipList<T>
where
    T: LogPosition + Send + Sync + 'static,
{
    map: Arc<SkipMap<Vec<u8>, T>>,
}

impl<T> SkipList<T>
where
    T: LogPosition + Send + Sync,
{
    pub fn new() -> Self {
        SkipList {
            map: Arc::new(SkipMap::new()),
        }
    }
}

impl<T> Index<T> for SkipList<T>
where
    T: LogPosition + Send + Sync + Copy,
{
    fn put(&self, key: Vec<u8>, pos: T) -> Option<T> {
        let mut result = None;
        if let Some(entry) = self.map.get(&key) {
            result = Some(*entry.value());
        }
        self.map.insert(key, pos);
        result
    }

    fn get(&self, key: Vec<u8>) -> Option<T> {
        if let Some(entry) = self.map.get(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<T> {
        if let Some(entry) = self.map.remove(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn list_keys(&self) -> crate::error::Result<Vec<Bytes>> {
        let mut keys = Vec::with_capacity(self.map.len());
        for e in self.map.iter() {
            keys.push(Bytes::copy_from_slice(e.key()));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator<T>> {
        let mut items = Vec::with_capacity(self.map.len());
        for entry in self.map.iter() {
            items.push((entry.key().clone(), *entry.value()))
        }
        if options.reverse {
            items.reverse();
        }
        Box::new(SkipListIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

pub struct SkipListIterator<T>
where
    T: LogPosition + Send + Sync,
{
    items: Vec<(Vec<u8>, T)>,
    curr_index: usize,
    options: IteratorOptions,
}

impl<T> IndexIterator<T> for SkipListIterator<T>
where
    T: LogPosition + Send + Sync,
{
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &T)> {
        if self.curr_index >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}
