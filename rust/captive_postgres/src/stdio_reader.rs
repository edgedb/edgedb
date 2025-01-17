use std::io::BufRead;
use std::sync::{Arc, RwLock};
use std::thread;

pub struct StdioReader {
    output: Arc<RwLock<String>>,
}

impl StdioReader {
    pub fn spawn<R: BufRead + Send + 'static>(reader: R, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        let output = Arc::new(RwLock::new(String::new()));
        let output_clone = Arc::clone(&output);

        thread::spawn(move || {
            let mut buf_reader = std::io::BufReader::new(reader);
            loop {
                let mut line = String::new();
                match buf_reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if let Ok(mut output) = output_clone.write() {
                            output.push_str(&line);
                        }
                        eprint!("[{}]: {}", prefix, line);
                    }
                    Err(e) => {
                        let error_line = format!("Error reading {}: {}\n", prefix, e);
                        if let Ok(mut output) = output_clone.write() {
                            output.push_str(&error_line);
                        }
                        eprintln!("{}", error_line);
                    }
                }
            }
        });

        StdioReader { output }
    }

    pub fn contains(&self, s: &str) -> bool {
        if let Ok(output) = self.output.read() {
            output.contains(s)
        } else {
            false
        }
    }
}
