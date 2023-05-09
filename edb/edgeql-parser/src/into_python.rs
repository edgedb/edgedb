use std::collections::HashMap;

use cpython::{
    PyBytes, PyDict, PyList, PyObject, PyResult, PyTuple, Python, PythonObject, ToPyObject,
};

/// Convert into a Python object.
///
/// Primitives (i64, String, Option, Vec) have this trait implemented with
/// calls to [cpython].
///
/// Structs have this trait derived to collect all their properties into a
/// [PyDict] and call constructor of the AST node.
///
/// Enums represent either enums, union types or child classes and thus have
/// three different derive implementations.
///
/// See [edgeql_parser_derive] crate.
pub trait IntoPython: Sized {
    fn into_python(self, py: Python<'_>, parent_kw_args: Option<PyDict>) -> PyResult<PyObject>;
}

impl IntoPython for String {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        Ok(self.to_py_object(py).into_object())
    }
}

impl IntoPython for i64 {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        Ok(self.to_py_object(py).into_object())
    }
}

impl IntoPython for f64 {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        Ok(self.to_py_object(py).into_object())
    }
}

impl IntoPython for bool {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        Ok(if self { py.True() } else { py.False() }.into_object())
    }
}

impl<T: IntoPython> IntoPython for Vec<T> {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        let mut elements = Vec::new();
        for x in self {
            elements.push(x.into_python(py, None)?);
        }
        Ok(PyList::new(py, elements.as_slice()).into_object())
    }
}

impl<T: IntoPython> IntoPython for Option<T> {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        if let Some(value) = self {
            value.into_python(py, None)
        } else {
            Ok(py.None())
        }
    }
}

impl<T: IntoPython> IntoPython for Box<T> {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        (*self).into_python(py, None)
    }
}

impl<T1: IntoPython, T2: IntoPython> IntoPython for (T1, T2) {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        let mut elements = Vec::new();
        elements.push(self.0.into_python(py, None)?);
        elements.push(self.1.into_python(py, None)?);
        Ok(PyTuple::new(py, elements.as_slice()).into_object())
    }
}

impl<K: IntoPython, V: IntoPython> IntoPython for HashMap<K, V> {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        for (key, value) in self {
            let key = key.into_python(py, None)?;
            let value = value.into_python(py, None)?;
            dict.set_item(py, key, value)?;
        }
        Ok(dict.into_object())
    }
}

impl IntoPython for Vec<u8> {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, self.as_slice()).into_object())
    }
}

impl IntoPython for () {
    fn into_python(self, py: Python<'_>, _: Option<PyDict>) -> PyResult<PyObject> {
        Ok(py.None().into_object())
    }
}

pub fn init_ast_class(
    py: Python,
    class_name: &'static str,
    kw_args: PyDict,
) -> Result<PyObject, cpython::PyErr> {
    let locals = PyDict::new(py);
    locals.set_item(py, "kw_args", kw_args)?;

    let code = format!("qlast.{class_name}(**kw_args)");
    py.eval(&code, None, Some(&locals))
}
