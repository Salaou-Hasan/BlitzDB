pub struct Page {
    pub id: u64,
    pub data: Vec<u8>,
}

pub struct PageId(pub u64);

impl Page {
    pub fn new(id: u64, size: usize) -> Self {
        Self {
            id,
            data: vec![0u8; size],
        }
    }
}
