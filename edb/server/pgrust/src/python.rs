use openssl::ssl::SslMethod;
use pyo3::{pymodule, types::PyModule, PyResult, Python};

#[pymodule]
fn _pg_rust(py: Python, m: &PyModule) -> PyResult<()> {
    let ctx = openssl::ssl::SslContextBuilder::new(SslMethod::tls_server()).unwrap().build();
    openssl::ssl::Ssl::new(&ctx).unwrap();
    Ok(())
}
