use cpython::{PyDict, PyObject, PyResult, PyString, Python};

use edgeql_parser::ast;

pub trait IntoPython: Sized {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject>;
}

impl IntoPython for ast::ReleaseSavepoint {
    fn into_python(self, py: Python<'_>) -> PyResult<PyObject> {
        let kw_args = PyDict::new(py);
        kw_args.set_item(py, "name", PyString::new(py, &self.name))?;

        init_ast_class(py, "ReleaseSavepoint", kw_args)
    }
}

fn init_ast_class(
    py: Python,
    class_name: &'static str,
    kw_args: PyDict,
) -> Result<PyObject, cpython::PyErr> {
    let locals = PyDict::new(py);
    locals.set_item(py, "kw_args", kw_args)?;

    let code = format!("qlast.{class_name}(**kw_args)");
    py.eval(&code, None, Some(&locals))
}
