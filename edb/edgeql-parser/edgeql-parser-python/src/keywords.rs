use pyo3::{prelude::*, types::PyFrozenSet};

use edgeql_parser::keywords;

pub struct AllKeywords {
    pub current: Py<PyFrozenSet>,
    pub future: Py<PyFrozenSet>,
    pub unreserved: Py<PyFrozenSet>,
    pub partial: Py<PyFrozenSet>,
}

pub fn get_keywords(py: Python) -> PyResult<AllKeywords> {
    let intern = py.import("sys")?.getattr("intern")?;

    Ok(AllKeywords {
        current: prepare_keywords(py, &keywords::CURRENT_RESERVED_KEYWORDS, &intern)?,
        unreserved: prepare_keywords(py, &keywords::UNRESERVED_KEYWORDS, &intern)?,
        future: prepare_keywords(py, &keywords::FUTURE_RESERVED_KEYWORDS, &intern)?,
        partial: prepare_keywords(py, &keywords::PARTIAL_RESERVED_KEYWORDS, &intern)?,
    })
}

fn prepare_keywords<'a, 'py, I: IntoIterator<Item = &'a &'static str>>(
    py: Python<'py>,
    keyword_set: I,
    intern: &Bound<'py, PyAny>,
) -> PyResult<Py<PyFrozenSet>> {
    PyFrozenSet::new(
        py,
        keyword_set
            .into_iter()
            .map(|s| intern.call((&s,), None).unwrap()),
    )
    .map(|o| o.unbind())
}
