use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::prelude::FileExt,
    path::PathBuf,
    sync::Arc,
};

use crate::error::{Errors, Result};
use log::error;
use parking_lot::RwLock;

use super::IOManager;

pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(filename: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(filename)
            .map_err(|e| {
                error!("failed to open data file: {}", e);
                return Errors::FailedToOpenDataFile;
            })?;

        Ok(FileIO {
            fd: Arc::new(RwLock::new(file)),
        })
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("read from data file err: {}", e);
                return Err(Errors::FailedReadFromDataFile);
            }
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("write to data file err: {}", e);
                return Err(Errors::FailedWriteToDataFile);
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            error!("failed to sync data file: {}", e);
            return Err(Errors::FailedSyncDataFile);
        }
        Ok(())
    }

    fn size(&self) -> u64 {
        let read_guard = self.fd.read();
        read_guard.metadata().unwrap().len()
    }
}
