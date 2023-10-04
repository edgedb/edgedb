use crate::cpython::PythonObjectWithTypeObject;

use cpython::exc::Exception;
use cpython::{
    PyClone, PyErr, PyList, PyObject, PyResult, PyType, Python, PythonObject, ToPyObject,
};
use edgeql_parser::tokenizer::Error;

// can't use py_exception macro because that fails on dotted module name
pub struct SyntaxError(PyObject);

pyobject_newtype!(SyntaxError);

impl SyntaxError {
    pub fn new<T: ToPyObject>(py: Python, args: T) -> PyErr {
        PyErr::new::<SyntaxError, T>(py, args)
    }
}

impl cpython::PythonObjectWithCheckedDowncast for SyntaxError {
    #[inline]
    fn downcast_from(
        py: Python,
        obj: PyObject,
    ) -> Result<SyntaxError, cpython::PythonObjectDowncastError> {
        if SyntaxError::type_object(py).is_instance(py, &obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(
                py,
                "SyntaxError",
                SyntaxError::type_object(py),
            ))
        }
    }

    #[inline]
    fn downcast_borrow_from<'a, 'p>(
        py: Python<'p>,
        obj: &'a PyObject,
    ) -> Result<&'a SyntaxError, cpython::PythonObjectDowncastError<'p>> {
        if SyntaxError::type_object(py).is_instance(py, obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_borrow_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(
                py,
                "SyntaxError",
                SyntaxError::type_object(py),
            ))
        }
    }
}

impl cpython::PythonObjectWithTypeObject for SyntaxError {
    #[inline]
    fn type_object(py: Python) -> PyType {
        unsafe {
            static mut TYPE_OBJECT: *mut cpython::_detail::ffi::PyTypeObject =
                0 as *mut cpython::_detail::ffi::PyTypeObject;

            if TYPE_OBJECT.is_null() {
                TYPE_OBJECT = PyErr::new_type(
                    py,
                    "edb._edgeql_parser.SyntaxError",
                    Some(PythonObject::into_object(py.get_type::<Exception>())),
                    None,
                )
                .as_type_ptr();
            }

            PyType::from_type_ptr(py, TYPE_OBJECT)
        }
    }
}

py_class!(pub class ParserResult |py| {
    data _out: PyObject;
    data _errors: PyList;

    def out(&self) -> PyResult<PyObject> {
        Ok(self._out(py).clone_ref(py))
    }
    def errors(&self) -> PyResult<PyList> {
        Ok(self._errors(py).clone_ref(py))
    }
});

pub fn parser_error_into_tuple(py: Python, error: Error) -> PyObject {
    (
        error.message,
        (error.span.start, error.span.end),
        error.hint,
        error.details,
    )
        .into_py_object(py)
        .into_object()
}
