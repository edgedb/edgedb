use cpython::{PyObject, ToPyObject, Python, PyErr, PythonObject, PyType};
use cpython::exc::Exception;
use crate::cpython::PythonObjectWithTypeObject;


// can't use py_exception macro because that fails on dotted module name
pub struct TokenizerError(PyObject);

pyobject_newtype!(TokenizerError);

impl TokenizerError {
    pub fn new<'p, T: ToPyObject>(py: Python<'p>, args: T) -> PyErr {
        PyErr::new::<TokenizerError, T>(py, args)
    }
}

impl cpython::PythonObjectWithCheckedDowncast for TokenizerError {
    #[inline]
    fn downcast_from<'p>(py: Python<'p>, obj: PyObject)
        -> Result<TokenizerError, cpython::PythonObjectDowncastError<'p>>
    {
        if TokenizerError::type_object(py).is_instance(py, &obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "TokenizerError",
                TokenizerError::type_object(py),
            ))
        }
    }

    #[inline]
    fn downcast_borrow_from<'a, 'p>(py: Python<'p>, obj: &'a PyObject)
        -> Result<&'a TokenizerError, cpython::PythonObjectDowncastError<'p>>
    {
        if TokenizerError::type_object(py).is_instance(py, obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_borrow_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "TokenizerError",
                TokenizerError::type_object(py),
            ))
        }
    }
}

impl cpython::PythonObjectWithTypeObject for TokenizerError {
    #[inline]
    fn type_object(py: Python) -> PyType {
        unsafe {
            static mut TYPE_OBJECT: *mut cpython::_detail::ffi::PyTypeObject
                = 0 as *mut cpython::_detail::ffi::PyTypeObject;

            if TYPE_OBJECT.is_null() {
                TYPE_OBJECT = PyErr::new_type(
                    py,
                    "edb._edgeql_rust.TokenizerError",
                    Some(PythonObject::into_object(py.get_type::<Exception>())),
                    None).as_type_ptr();
            }

            PyType::from_type_ptr(py, TYPE_OBJECT)
        }
    }
}
