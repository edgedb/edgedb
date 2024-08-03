#[derive(Debug)]
pub struct BufWriter<'a> {
    buf: &'a mut [u8],
    size: usize,
    error: bool,
}

impl<'a> BufWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            size: 0,
            error: false,
        }
    }

    pub fn test(&mut self, size: usize) -> bool {
        if self.buf.len() < size {
            self.size += size;
            self.error = true;
            false
        } else {
            true
        }
    }

    pub fn write(&mut self, buf: &[u8]) {
        let len = buf.len();
        self.size += len;
        if self.error {
            return;
        }
        if self.buf.len() < len {
            self.error = true;
            return;
        }
        let b = std::mem::take(&mut self.buf);
        let (write, rest) = b.split_at_mut(len);
        write.copy_from_slice(buf);
        self.buf = rest;
    }

    pub fn write_u8(&mut self, value: u8) {
        self.size += 1;
        if self.error {
            return;
        }
        if self.buf.is_empty() {
            self.error = true;
            return;
        }
        let b = std::mem::take(&mut self.buf);
        let (write, rest) = b.split_at_mut(1);
        write[0] = value;
        self.buf = rest;
    }

    pub fn finish(self) -> Result<usize, usize> {
        if self.error {
            Err(self.size)
        } else {
            Ok(self.size)
        }
    }
}
