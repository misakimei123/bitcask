pub mod file_io;
pub mod mmap;
use std::path::PathBuf;

use file_io::FileIO;
use mmap::MMapIO;

use crate::{error::Result, option::IOType};

// 抽象 IO 管理接口
pub trait IOManager: Sync + Send {
    // 从文件的给定位置读取对应的数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    // 写入数据到文件
    fn write(&self, buf: &[u8]) -> Result<usize>;

    // 同步数据
    fn sync(&self) -> Result<()>;

    // 获取文件大小
    fn size(&self) -> u64;
}

pub fn new_io_manager(file_name: PathBuf, io_type: IOType) -> Box<dyn IOManager> {
    match io_type {
        IOType::StandardFIO => Box::new(FileIO::new(file_name).unwrap()),
        IOType::MemoryMap => Box::new(MMapIO::new(file_name).unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::error::Errors;

    use super::*;

    fn test_write(io: Box<dyn IOManager>) {
        let res1 = io.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());

        let res2 = io.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());
    }

    #[test]
    fn test_file_io_write() {
        let path = "/tmp/a.data";
        let fio = new_io_manager(PathBuf::from(path), IOType::StandardFIO);
        test_write(fio);
        let res = fs::remove_file(path);
        assert!(res.is_ok());
    }

    fn test_read(io: Box<dyn IOManager>) {
        let res1 = io.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());

        let res2 = io.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let mut buf = [0u8; 5];
        let read_res1 = io.read(&mut buf, 0);
        assert!(read_res1.is_ok());
        assert_eq!(5, read_res1.ok().unwrap());

        let mut buf2 = [0u8; 5];
        let read_res2 = io.read(&mut buf2, 5);
        assert!(read_res2.is_ok());
        assert_eq!(5, read_res2.ok().unwrap());
    }

    #[test]
    fn test_file_io_read() {
        let path = "/tmp/b.data";
        let fio = new_io_manager(PathBuf::from(path), IOType::StandardFIO);
        test_read(fio);
        let res = fs::remove_file(path);
        assert!(res.is_ok());
    }

    fn test_sync(io: Box<dyn IOManager>) {
        let res1 = io.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());

        let res2 = io.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let sync_res = io.sync();
        assert!(sync_res.is_ok());
    }

    #[test]
    fn test_file_io_sync() {
        let path = "/tmp/c.data";
        let fio = new_io_manager(PathBuf::from(path), IOType::StandardFIO);
        test_sync(fio);
        let res = fs::remove_file(path);
        assert!(res.is_ok());
    }

    fn test_size(io: Box<dyn IOManager>) {
        let size1 = io.size();
        assert_eq!(size1, 0);

        let res2 = io.write("key-b".as_bytes());
        assert!(res2.is_ok());

        let size2 = io.size();
        assert_eq!(size2, 5);
    }

    #[test]
    fn test_file_io_size() {
        let path = "/tmp/d.data";
        let fio = new_io_manager(PathBuf::from(path), IOType::StandardFIO);
        test_size(fio);
        let res = fs::remove_file(path);
        assert!(res.is_ok());
    }

    #[test]
    fn test_mmap_read() {
        let path = PathBuf::from("/tmp/mmap-test.data");

        // 文件为空
        let mmap_res1 = MMapIO::new(path.clone());
        assert!(mmap_res1.is_ok());
        let mmap_io1 = mmap_res1.ok().unwrap();
        let mut buf1 = [0u8; 10];
        let read_res1 = mmap_io1.read(&mut buf1, 0);
        assert_eq!(read_res1.err().unwrap(), Errors::ReadDataFileEOF);

        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();
        fio.write(b"aa").unwrap();
        fio.write(b"bb").unwrap();
        fio.write(b"cc").unwrap();

        // 有数据的情况
        let mmap_res2 = MMapIO::new(path.clone());
        assert!(mmap_res2.is_ok());
        let mmap_io2 = mmap_res2.ok().unwrap();

        let mut buf2 = [0u8; 2];
        let read_res2 = mmap_io2.read(&mut buf2, 2);
        assert!(read_res2.is_ok());

        let mut buf3 = [0u8; 2];
        let read_res2 = mmap_io2.read(&mut buf3, 10);
        assert!(read_res2.is_err());

        let remove_res = fs::remove_file(path.clone());
        assert!(remove_res.is_ok());
    }
}
