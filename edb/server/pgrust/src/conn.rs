trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite {}

struct PGConn<S: Stream> {
    stm: S
}

impl<S: Stream> PGConn<S> {
    pub fn new(stm: S) -> Self {
        Self { stm }
    }

    
}
