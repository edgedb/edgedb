use cpython::{PyObject, PyResult, PyString, PyTuple, Python, ToPyObject};
use edgeql_parser::{into_python::IntoPython, parser};

pub fn parse_block(py: Python, source: &PyString) -> PyResult<PyTuple> {
    let source = source.to_string(py)?;

    // parse
    let res = parser::parse_block(&source);

    // convert into a Python object
    let ast = res
        .ast
        .map(|ast| ast.into_python(py, None))
        .transpose()?
        .into_py_object(py);

    compose_result(py, ast, res.errors)
}

pub fn parse_single(py: Python, source: &PyString) -> PyResult<PyTuple> {
    let source = source.to_string(py)?;

    // parse
    let res = parser::parse_single(&source);

    // convert into a Python object
    let ast = res.ast.into_python(py, None)?;

    compose_result(py, ast, res.errors)
}

fn compose_result(py: Python, ast: PyObject, errors: Vec<parser::Error>) -> PyResult<PyTuple> {
    let errors = errors
        .into_iter()
        .map(|e| err_to_py_object(e, py))
        .collect::<Vec<_>>()
        .into_py_object(py);

    Ok((ast, errors).into_py_object(py))
}

fn err_to_py_object(err: parser::Error, py: Python) -> PyTuple {
    (err.message, err.span.start, err.span.end).into_py_object(py)
}
