pub mod data_file;
pub mod log_record;

pub trait LogPosition {
    fn get_size(&self) -> u32;
}

impl LogPosition for log_record::LogRecordPos {
    fn get_size(&self) -> u32 {
        self.size
    }
}
