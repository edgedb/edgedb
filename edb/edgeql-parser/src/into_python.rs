use std::collections::HashMap;

use cpython::{
    PyBytes, PyDict, PyList, PyObject, PyResult, PyTuple, Python, PythonObject, ToPyObject,
};

// use crate::ast;

pub trait IntoPython: Sized {
    fn into_python(self, py: Python<'_>, parent_kw_args: Option<PyDict>) -> PyResult<PyObject>;
}

// impl IntoPython for ast::Transaction {
//     fn into_python(self, py: Python<'_>, parent_kw_args: Option<PyDict>) -> PyResult<PyObject> {
//         let kw_args = PyDict::new(py);
//         // here we set fields other than `kind`

//         self.kind.into_python(py, Some(kw_args))
//     }
// }

// impl IntoPython for ast::TransactionKind {
//     fn into_python(self, py: Python<'_>, parent_kw_args: Option<PyDict>) -> PyResult<PyObject> {
//         match self {
//             ast::TransactionKind::StartTransaction(x) => x.into_python(py, parent_kw_args),
//             ast::TransactionKind::CommitTransaction(x) => x.into_python(py, parent_kw_args),
//             ast::TransactionKind::RollbackTransaction(x) => x.into_python(py, parent_kw_args),
//             ast::TransactionKind::DeclareSavepoint(x) => x.into_python(py, parent_kw_args),
//             ast::TransactionKind::RollbackToSavepoint(x) => x.into_python(py, parent_kw_args),
//             ast::TransactionKind::ReleaseSavepoint(x) => x.into_python(py, parent_kw_args),
//         }
//     }
// }

// impl IntoPython for ast::ReleaseSavepoint {
//     fn into_python(self, py: Python<'_>, parent_kw_args: Option<PyDict>) -> PyResult<PyObject> {
//         let kw_args = parent_kw_args.unwrap_or_else(|| PyDict::new(py));
//         kw_args.set_item(py, "name", PyString::new(py, &self.name))?;

//         init_ast_class(py, "ReleaseSavepoint", kw_args)
//     }
// }

// impl<T: ToPyObject> IntoPython for T {
//     fn into_python(self, py: Python<'_>, parent_kw_args: Option<PyDict>) -> PyResult<PyObject> {
//         Ok(self.to_py_object(py).into_object())
//     }
// }

// impl<T: IntoPython> ToPyObject for T {
//     type ObjectType = PyObject;

//     fn to_py_object(&self, py: Python) -> Self::ObjectType {
//         todo!()
//     }
// }

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
