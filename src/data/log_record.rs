use bytes::{BufMut, BytesMut};
use prost::{
    encode_length_delimiter,
    encoding::{decode_varint, encode_varint},
    length_delimiter_len,
};

#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32, // 文件 id，表示将数据存储到了哪个文件当中
    pub(crate) offset: u64,  // 偏移，表示将数据存储到了数据文件中的哪个位置
    pub(crate) size: u32,    // 数据在磁盘上的占据的空间大小
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum LogRecordType {
    // 正常 put 的数据
    NORMAL = 1,

    // 删除标记 墓碑
    DELETED = 2,
    // 事务完成标记
    TXNFINISHED = 3,
}

#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

// 从数据文件中读取的 log_record 信息，包含其 size
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

// 暂存事务数据信息
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

//	+----------+-------------------------+----------------------+--------------+--------------+--------+
//	|  type    |    key size             |   value size         |       key    |      value   |  crc32   |
//	+----------+-------------------------+----------------------+--------------+--------------+--------+
//	  1byte       varint（max size 5）       varint（max size 5）     key len      value len      4byte
impl LogRecord {
    pub fn encode(&self) -> Vec<u8> {
        let (enc_buf, _) = self.encode_and_get_crc();
        enc_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc_value) = self.encode_and_get_crc();
        crc_value
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        // 先存入type
        buf.put_u8(self.rec_type as u8);

        // 再存入变长的key和value长度
        encode_length_delimiter(self.key.len(), &mut buf).expect("encode key len error");
        encode_length_delimiter(self.value.len(), &mut buf).expect("encode value len error");

        // 存储key和value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 最后存储crc校验值
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        (buf.to_vec(), crc)
    }

    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + std::mem::size_of::<u32>()
    }
}

impl From<u8> for LogRecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            3 => LogRecordType::TXNFINISHED,
            _ => panic!("unknown log record type"),
        }
    }
}

impl LogRecordPos {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.offset as u64, &mut buf);
        encode_varint(self.size as u64, &mut buf);
        buf.to_vec()
    }

    // pub fn decode(pos: Vec<u8>) -> Self {
    //     let mut buf = BytesMut::new();
    //     buf.put_slice(&pos);

    //     let fid = match decode_varint(&mut buf) {
    //         Ok(fid) => fid,
    //         Err(e) => panic!("decode log record pos err: {}", e),
    //     };
    //     let offset = match decode_varint(&mut buf) {
    //         Ok(offset) => offset,
    //         Err(e) => panic!("decode log record pos err: {}", e),
    //     };
    //     let size = match decode_varint(&mut buf) {
    //         Ok(size) => size,
    //         Err(e) => panic!("decode log record pos err: {}", e),
    //     };
    //     LogRecordPos {
    //         file_id: fid as u32,
    //         offset,
    //         size: size as u32,
    //     }
    // }
}

// 解码 LogRecordPos
pub fn decode_log_record_pos(pos: Vec<u8>) -> LogRecordPos {
    let mut buf = BytesMut::new();
    buf.put_slice(&pos);

    let fid = match decode_varint(&mut buf) {
        Ok(fid) => fid,
        Err(e) => panic!("decode log record pos err: {}", e),
    };
    let offset = match decode_varint(&mut buf) {
        Ok(offset) => offset,
        Err(e) => panic!("decode log record pos err: {}", e),
    };
    let size = match decode_varint(&mut buf) {
        Ok(size) => size,
        Err(e) => panic!("decode log record pos err: {}", e),
    };
    LogRecordPos {
        file_id: fid as u32,
        offset,
        size: size as u32,
    }
}

// 获取 LogRecord header 部分的最大长度
pub fn max_log_record_header_size() -> usize {
    // 1byte + 5byte + 5byte
    std::mem::size_of::<u8>()
        + length_delimiter_len(std::u32::MAX as usize)
        + length_delimiter_len(std::u32::MAX as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_and_crc() {
        // 正常的一条 LogRecord 编码
        let rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc1 = rec1.encode();
        assert!(enc1.len() > 5);
        assert_eq!(1020360578, rec1.get_crc());

        // LogRecord 的 value 为空
        let rec2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc2 = rec2.encode();
        assert!(enc2.len() > 5);
        assert_eq!(3756865478, rec2.get_crc());

        // 类型为 Deleted 的情况
        let rec3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::DELETED,
        };
        let enc3 = rec3.encode();
        assert!(enc3.len() > 5);
        assert_eq!(1867197446, rec3.get_crc());
    }
}
