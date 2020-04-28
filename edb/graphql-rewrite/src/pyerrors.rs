use cpython::{PyObject, ToPyObject, Python, PyErr, PythonObject, PyType};
use cpython::exc::Exception;
use crate::cpython::PythonObjectWithTypeObject;


// can't use py_exception macro because that fails on dotted module name
pub struct LexingError(PyObject);
pub struct SyntaxError(PyObject);
pub struct NotFoundError(PyObject);
pub struct AssertionError(PyObject);
pub struct QueryError(PyObject);

pyobject_newtype!(LexingError);
pyobject_newtype!(SyntaxError);
pyobject_newtype!(NotFoundError);
pyobject_newtype!(AssertionError);
pyobject_newtype!(QueryError);

impl LexingError {
    pub fn new<'p, T: ToPyObject>(py: Python<'p>, args: T) -> PyErr {
        PyErr::new::<LexingError, T>(py, args)
    }
}

impl cpython::PythonObjectWithCheckedDowncast for LexingError {
    #[inline]
    fn downcast_from<'p>(py: Python<'p>, obj: PyObject)
        -> Result<LexingError, cpython::PythonObjectDowncastError<'p>>
    {
        if LexingError::type_object(py).is_instance(py, &obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "LexingError",
                LexingError::type_object(py),
            ))
        }
    }

    #[inline]
    fn downcast_borrow_from<'a, 'p>(py: Python<'p>, obj: &'a PyObject)
        -> Result<&'a LexingError, cpython::PythonObjectDowncastError<'p>>
    {
        if LexingError::type_object(py).is_instance(py, obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_borrow_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "LexingError",
                LexingError::type_object(py),
            ))
        }
    }
}

impl cpython::PythonObjectWithTypeObject for LexingError {
    #[inline]
    fn type_object(py: Python) -> PyType {
        unsafe {
            static mut TYPE_OBJECT: *mut cpython::_detail::ffi::PyTypeObject
                = 0 as *mut cpython::_detail::ffi::PyTypeObject;

            if TYPE_OBJECT.is_null() {
                TYPE_OBJECT = PyErr::new_type(
                    py,
                    "edb._graphql_rewrite.LexingError",
                    Some(PythonObject::into_object(py.get_type::<Exception>())),
                    None).as_type_ptr();
            }

            PyType::from_type_ptr(py, TYPE_OBJECT)
        }
    }
}

impl SyntaxError {
    pub fn new<'p, T: ToPyObject>(py: Python<'p>, args: T) -> PyErr {
        PyErr::new::<SyntaxError, T>(py, args)
    }
}

impl cpython::PythonObjectWithCheckedDowncast for SyntaxError {
    #[inline]
    fn downcast_from<'p>(py: Python<'p>, obj: PyObject)
        -> Result<SyntaxError, cpython::PythonObjectDowncastError<'p>>
    {
        if SyntaxError::type_object(py).is_instance(py, &obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "SyntaxError",
                SyntaxError::type_object(py),
            ))
        }
    }

    #[inline]
    fn downcast_borrow_from<'a, 'p>(py: Python<'p>, obj: &'a PyObject)
        -> Result<&'a SyntaxError, cpython::PythonObjectDowncastError<'p>>
    {
        if SyntaxError::type_object(py).is_instance(py, obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_borrow_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
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
            static mut TYPE_OBJECT: *mut cpython::_detail::ffi::PyTypeObject
                = 0 as *mut cpython::_detail::ffi::PyTypeObject;

            if TYPE_OBJECT.is_null() {
                TYPE_OBJECT = PyErr::new_type(
                    py,
                    "edb._graphql_rewrite.SyntaxError",
                    Some(PythonObject::into_object(py.get_type::<Exception>())),
                    None).as_type_ptr();
            }

            PyType::from_type_ptr(py, TYPE_OBJECT)
        }
    }
}

impl NotFoundError {
    pub fn new<'p, T: ToPyObject>(py: Python<'p>, args: T) -> PyErr {
        PyErr::new::<NotFoundError, T>(py, args)
    }
}

impl cpython::PythonObjectWithCheckedDowncast for NotFoundError {
    #[inline]
    fn downcast_from<'p>(py: Python<'p>, obj: PyObject)
        -> Result<NotFoundError, cpython::PythonObjectDowncastError<'p>>
    {
        if NotFoundError::type_object(py).is_instance(py, &obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "NotFoundError",
                NotFoundError::type_object(py),
            ))
        }
    }

    #[inline]
    fn downcast_borrow_from<'a, 'p>(py: Python<'p>, obj: &'a PyObject)
        -> Result<&'a NotFoundError, cpython::PythonObjectDowncastError<'p>>
    {
        if NotFoundError::type_object(py).is_instance(py, obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_borrow_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "NotFoundError",
                NotFoundError::type_object(py),
            ))
        }
    }
}

impl cpython::PythonObjectWithTypeObject for NotFoundError {
    #[inline]
    fn type_object(py: Python) -> PyType {
        unsafe {
            static mut TYPE_OBJECT: *mut cpython::_detail::ffi::PyTypeObject
                = 0 as *mut cpython::_detail::ffi::PyTypeObject;

            if TYPE_OBJECT.is_null() {
                TYPE_OBJECT = PyErr::new_type(
                    py,
                    "edb._graphql_rewrite.NotFoundError",
                    Some(PythonObject::into_object(py.get_type::<Exception>())),
                    None).as_type_ptr();
            }

            PyType::from_type_ptr(py, TYPE_OBJECT)
        }
    }
}

impl AssertionError {
    pub fn new<'p, T: ToPyObject>(py: Python<'p>, args: T) -> PyErr {
        PyErr::new::<AssertionError, T>(py, args)
    }
}

impl cpython::PythonObjectWithCheckedDowncast for AssertionError {
    #[inline]
    fn downcast_from<'p>(py: Python<'p>, obj: PyObject)
        -> Result<AssertionError, cpython::PythonObjectDowncastError<'p>>
    {
        if AssertionError::type_object(py).is_instance(py, &obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "AssertionError",
                AssertionError::type_object(py),
            ))
        }
    }

    #[inline]
    fn downcast_borrow_from<'a, 'p>(py: Python<'p>, obj: &'a PyObject)
        -> Result<&'a AssertionError, cpython::PythonObjectDowncastError<'p>>
    {
        if AssertionError::type_object(py).is_instance(py, obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_borrow_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "AssertionError",
                AssertionError::type_object(py),
            ))
        }
    }
}

impl cpython::PythonObjectWithTypeObject for AssertionError {
    #[inline]
    fn type_object(py: Python) -> PyType {
        unsafe {
            static mut TYPE_OBJECT: *mut cpython::_detail::ffi::PyTypeObject
                = 0 as *mut cpython::_detail::ffi::PyTypeObject;

            if TYPE_OBJECT.is_null() {
                TYPE_OBJECT = PyErr::new_type(
                    py,
                    "edb._graphql_rewrite.AssertionError",
                    Some(PythonObject::into_object(py.get_type::<Exception>())),
                    None).as_type_ptr();
            }

            PyType::from_type_ptr(py, TYPE_OBJECT)
        }
    }
}

impl QueryError {
    pub fn new<'p, T: ToPyObject>(py: Python<'p>, args: T) -> PyErr {
        PyErr::new::<QueryError, T>(py, args)
    }
}

impl cpython::PythonObjectWithCheckedDowncast for QueryError {
    #[inline]
    fn downcast_from<'p>(py: Python<'p>, obj: PyObject)
        -> Result<QueryError, cpython::PythonObjectDowncastError<'p>>
    {
        if QueryError::type_object(py).is_instance(py, &obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "QueryError",
                QueryError::type_object(py),
            ))
        }
    }

    #[inline]
    fn downcast_borrow_from<'a, 'p>(py: Python<'p>, obj: &'a PyObject)
        -> Result<&'a QueryError, cpython::PythonObjectDowncastError<'p>>
    {
        if QueryError::type_object(py).is_instance(py, obj) {
            Ok(unsafe { PythonObject::unchecked_downcast_borrow_from(obj) })
        } else {
            Err(cpython::PythonObjectDowncastError::new(py,
                "QueryError",
                QueryError::type_object(py),
            ))
        }
    }
}

impl cpython::PythonObjectWithTypeObject for QueryError {
    #[inline]
    fn type_object(py: Python) -> PyType {
        unsafe {
            static mut TYPE_OBJECT: *mut cpython::_detail::ffi::PyTypeObject
                = 0 as *mut cpython::_detail::ffi::PyTypeObject;

            if TYPE_OBJECT.is_null() {
                TYPE_OBJECT = PyErr::new_type(
                    py,
                    "edb._graphql_rewrite.QueryError",
                    Some(PythonObject::into_object(py.get_type::<Exception>())),
                    None).as_type_ptr();
            }

            PyType::from_type_ptr(py, TYPE_OBJECT)
        }
    }
}
