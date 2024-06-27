pub mod skiplist;

use std::path::PathBuf;

use bytes::Bytes;
use skiplist::SkipList;

use crate::{
    data::{log_record::LogRecordPos, LogPosition},
    error::Result,
    option::{IndexType, IteratorOptions},
};

// Index 抽象索引接口，
pub trait Index<T>: Sync + Send
where
    T: LogPosition,
{
    // 向索引中存储 key 对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: T) -> Option<T>;

    // 根据 key 取出对应的索引位置信息
    fn get(&self, key: Vec<u8>) -> Option<T>;

    // 根据 key 删除对应的索引位置信息
    fn delete(&self, key: Vec<u8>) -> Option<T>;

    // 获取索引存储的所有的 key
    fn list_keys(&self) -> Result<Vec<Bytes>>;

    // 返回索引迭代器
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator<T>>;
}

pub fn new_indexer<T: LogPosition>(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Index<T>>
where
    skiplist::SkipList<LogRecordPos>: Index<T>,
{
    match index_type {
        IndexType::SkipList => {
            let skl = SkipList::<LogRecordPos>::new();
            let index = Box::new(skl);
            index
        }
    }
}

pub trait IndexIterator<T>: Sync + Send
where
    T: LogPosition,
{
    // Rewind 重新回到迭代器的起点，即第一个数据
    fn rewind(&mut self);

    // Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
    fn seek(&mut self, key: Vec<u8>);

    // Next 跳转到下一个 key，返回 None 则说明迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &T)>;
}

#[cfg(test)]
mod tests {
    use skiplist::SkipList;

    use crate::data::log_record::LogRecordPos;

    use super::*;

    fn test_put(index: Box<dyn Index<LogRecordPos>>) {
        let res1 = index.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = index.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res2.is_none());
        let res3 = index.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res3.is_none());
        let res4 = index.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res4.is_none());

        let res5 = index.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 93,
                offset: 22,
                size: 11,
            },
        );
        assert!(res5.is_some());
        let v = res5.unwrap();
        assert_eq!(v.file_id, 1123);
        assert_eq!(v.offset, 1232);
    }

    #[test]
    fn test_skl_put() {
        let skl = SkipList::new();
        let index = Box::new(skl);
        test_put(index);
    }

    fn test_get(index: Box<dyn Index<LogRecordPos>>) {
        let v1 = index.get(b"not exists".to_vec());
        assert!(v1.is_none());

        let res1 = index.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let v2 = index.get(b"aacd".to_vec());
        assert!(v2.is_some());
        assert_eq!(v2.unwrap().file_id, 1123);
        assert_eq!(v2.unwrap().offset, 1232);
        assert_eq!(v2.unwrap().size, 11);

        let res2 = index.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 990,
                size: 11,
            },
        );
        assert!(res2.is_some());
        let v3 = index.get(b"aacd".to_vec());
        assert!(v3.is_some());
    }

    #[test]
    fn test_skl_get() {
        let skl = SkipList::new();
        let index = Box::new(skl);
        test_get(index);
    }

    fn test_delete(index: Box<dyn Index<LogRecordPos>>) {
        let r1 = index.delete(b"not exists".to_vec());
        assert!(r1.is_none());

        let res1 = index.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());

        let r2 = index.delete(b"aacd".to_vec());
        assert!(r2.is_some());
        let v = r2.unwrap();
        assert_eq!(v.file_id, 1123);
        assert_eq!(v.offset, 1232);

        let v2 = index.get(b"aacd".to_vec());
        assert!(v2.is_none());
    }

    #[test]
    fn test_skl_delete() {
        let skl = SkipList::new();
        let index = Box::new(skl);
        test_delete(index);
    }

    fn test_keys(index: Box<dyn Index<LogRecordPos>>) {
        let keys1 = index.list_keys();
        assert_eq!(keys1.ok().unwrap().len(), 0);

        let res1 = index.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = index.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res2.is_none());
        let res3 = index.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res3.is_none());
        let res4 = index.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res4.is_none());

        let keys2 = index.list_keys();
        assert_eq!(keys2.ok().unwrap().len(), 4);
    }

    #[test]
    fn test_skl_list_keys() {
        let skl = SkipList::new();
        let index = Box::new(skl);
        test_keys(index);
    }

    fn test_iterator(index: Box<dyn Index<LogRecordPos>>) {
        let res1 = index.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = index.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res2.is_none());
        let res3 = index.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res3.is_none());
        let res4 = index.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res4.is_none());

        let mut opts = IteratorOptions::default();
        opts.reverse = true;
        let mut iter1 = index.iterator(opts);

        while let Some((key, _)) = iter1.next() {
            assert!(!key.is_empty());
        }
    }

    #[test]
    fn test_skl_iterator() {
        let skl = SkipList::new();
        let index = Box::new(skl);
        test_iterator(index);
    }
}
