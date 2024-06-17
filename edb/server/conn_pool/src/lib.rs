pub(crate) mod algo;
pub(crate) mod block;
pub(crate) mod conn;
pub(crate) mod pool;
#[cfg(test)]
pub mod test;
pub(crate) mod waitqueue;

#[cfg(feature = "python_extension")]
mod python {
    use std::{
        collections::HashMap,
        result,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    };

    use pyo3::prelude::*;

    use crate::conn;

    #[pyclass]
    struct ConnPool {
        callback: PyObject,
        /// Our RPC-like response callbacks.
        responses: Arc<RwLock<HashMap<usize, tokio::sync::oneshot::Sender<PyObject>>>>,
        id: Arc<AtomicUsize>,
    }

    #[pymethods]
    impl ConnPool {
        #[new]
        fn new(py: Python, callback: PyObject) -> Self {
            ConnPool {
                callback,
                responses: Default::default(),
                id: Default::default(),
            }
        }

        fn _respond(&self, py: Python, response_id: usize, object: PyObject) {
            println!(" - Sending!");
            let response = self.responses.write().unwrap().remove(&response_id);
            if let Some(response) = response {
                response.send(object).unwrap();
            } else {
                println!("Missing?");
            }
            println!(" - Sent!");
        }

        fn halt(&self, py: Python) {}

        fn run(&self, py: Python) {
            let callback = self.callback.clone();
            let id = self.id.clone();
            let responses = self.responses.clone();
            py.allow_threads(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                let task2 = rt.spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        let result = Python::with_gil(|py| {
                            println!("1a. Rust. Calling from Rust -> Python");
                            let (sender, receiver) = tokio::sync::oneshot::channel();
                            let response_id = id.fetch_add(1, Ordering::SeqCst);
                            responses.write().unwrap().insert(response_id, sender);
                            let Ok(result) = callback.call(py, (0, response_id, 1), None) else {
                                println!("Error?");
                                return false;
                            };
                            let Ok(result) = result.is_true(py) else {
                                println!("Error?");
                                return false;
                            };
                            if !result {
                                return false;
                            }
                            println!("1b. Rust. Done");

                            tokio::task::spawn(async {
                                let obj = receiver.await.unwrap();
                                Python::with_gil(|py| {
                                    println!("4. Rust. Received {}", obj.to_string());
                                })
                            });
                            true
                        });
                        if !result {
                            break;
                        }
                    }
                });
                rt.block_on(async move {
                    task2.await;
                })
            })
        }
    }

    #[pymodule]
    fn _conn_pool(py: Python, m: &PyModule) -> PyResult<()> {
        m.add_class::<ConnPool>().unwrap();
        Ok(())
    }

    #[pymodule]
    fn _conn_pool2(py: Python, m: &PyModule) -> PyResult<()> {
        m.add_class::<ConnPool>().unwrap();
        Ok(())
    }
}
