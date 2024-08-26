use pyo3::{
    prelude::*,
    types::{PyList, PyString},
};

use edgeql_parser::keywords;

pub struct AllKeywords {
    pub current: PyObject,
    pub future: PyObject,
    pub unreserved: PyObject,
    pub partial: PyObject,
}

pub fn get_keywords(py: Python) -> PyResult<AllKeywords> {
    let intern = py.import_bound("sys")?.getattr("intern")?;
    let frozen = py.import_bound("builtins")?.getattr("frozenset")?;

    let current = prepare_keywords(py, keywords::CURRENT_RESERVED_KEYWORDS.iter(), &intern)?;
    let unreserved = prepare_keywords(py, keywords::UNRESERVED_KEYWORDS.iter(), &intern)?;
    let future = prepare_keywords(py, keywords::FUTURE_RESERVED_KEYWORDS.iter(), &intern)?;
    let partial = prepare_keywords(py, keywords::PARTIAL_RESERVED_KEYWORDS.iter(), &intern)?;
    Ok(AllKeywords {
        current: frozen
            .call((PyList::new_bound(py, &current),), None)?
            .into(),
        unreserved: frozen
            .call((PyList::new_bound(py, &unreserved),), None)?
            .into(),
        future: frozen.call((PyList::new_bound(py, &future),), None)?.into(),
        partial: frozen
            .call((PyList::new_bound(py, &partial),), None)?
            .into(),
    })
}

fn prepare_keywords<'py, I: Iterator<Item = &'py &'static str>>(
    py: Python<'py>,
    keyword_set: I,
    intern: &'py Bound<'py, PyAny>,
) -> Result<Vec<Bound<'py, PyAny>>, PyErr> {
    keyword_set
        .cloned()
        .map(|s: &str| intern.call((PyString::new_bound(py, s),), None))
        .collect()
}
