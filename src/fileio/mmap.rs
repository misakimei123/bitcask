use super::IOManager;
use crate::error::{Errors, Result};
use log::error;
use memmap2::Mmap;
use parking_lot::Mutex;
use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

pub struct MMapIO {
    map: Arc<Mutex<Mmap>>,
}

impl MMapIO {
    pub fn new(filename: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .map_err(|e| {
                error!("failed to open data file: {}", e);
                return Errors::FailedToOpenDataFile;
            })?;
        let map = unsafe { Mmap::map(&file).expect("failed to map the file") };

        Ok(MMapIO {
            map: Arc::new(Mutex::new(map)),
        })
    }
}

impl IOManager for MMapIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let map_arr = self.map.lock();
        let end = offset + buf.len() as u64;
        if end > map_arr.len() as u64 {
            return Err(Errors::ReadDataFileEOF);
        }
        let val = &map_arr[offset as usize..end as usize];
        buf.copy_from_slice(val);
        Ok(val.len())
    }

    fn write(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    fn size(&self) -> u64 {
        let map_arr = self.map.lock();
        map_arr.len() as u64
    }
}
