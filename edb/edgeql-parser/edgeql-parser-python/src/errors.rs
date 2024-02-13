use edgeql_parser::tokenizer::Error;
use pyo3::prelude::*;
use pyo3::{create_exception, exceptions};

create_exception!(_edgeql_parser, SyntaxError, exceptions::PyException);

#[pyclass]
pub struct ParserResult {
    #[pyo3(get)]
    pub out: PyObject,

    #[pyo3(get)]
    pub errors: PyObject,
}

pub fn parser_error_into_tuple(py: Python, error: Error) -> PyObject {
    (
        error.message,
        (error.span.start, error.span.end),
        error.hint,
        error.details,
    )
        .into_py(py)
}
