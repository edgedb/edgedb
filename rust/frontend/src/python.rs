#[cfg(test)]
mod tests {
    use pyo3::{types::PyAnyMethods, Python};

    use super::*;

    #[test]
    fn test_python_extension() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            let version = sys.getattr("version").unwrap();
            println!("Python version: {}", version);
        });
    }
}
