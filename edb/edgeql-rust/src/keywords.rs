use cpython::{Python, ObjectProtocol, PyResult, PyString, PyList, PyObject};

use edgeql_parser::keywords;

pub struct AllKeywords {
    pub current: PyObject,
    pub future: PyObject,
    pub unreserved: PyObject,
}


pub fn get_keywords(py: Python) -> PyResult<AllKeywords> {
    let py_intern = py.import("sys")?.get(py, "intern")?;
    let py_frozenset = py.import("builtins")?.get(py, "frozenset")?;
    let intern = |s: &str| -> PyResult<PyObject> {
        let py_str = PyString::new(py, s);
        py_intern.call(py, (py_str,), None)
    };
    let current = keywords::CURRENT_RESERVED_KEYWORDS
        .iter()
        .map(|x| *x).map(&intern)
        .collect::<Result<Vec<_>,_>>()?;
    let unreserved = keywords::UNRESERVED_KEYWORDS
        .iter()
        .map(|x| *x).map(&intern)
        .collect::<Result<Vec<_>,_>>()?;
    let future = keywords::FUTURE_RESERVED_KEYWORDS
        .iter()
        .map(|x| *x).map(&intern)
        .collect::<Result<Vec<_>,_>>()?;
    Ok(AllKeywords {
        current: py_frozenset.call(py, (PyList::new(py, &current),), None)?,
        unreserved: py_frozenset.call(py, (PyList::new(py, &unreserved),), None)?,
        future: py_frozenset.call(py, (PyList::new(py, &future),), None)?,
    })
}

