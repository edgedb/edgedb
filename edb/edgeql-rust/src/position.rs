
use cpython::exc::{RuntimeError};
use cpython::{py_class, PyErr, PyResult, PyInt, PyBytes, PyList, ToPyObject};
use cpython::{PyObject};

use edgeql_parser::position::InflatedPos;

py_class!(pub class SourcePoint |py| {
    data _position: InflatedPos;
    @classmethod def from_offsets(cls, data: PyBytes, offsets: PyObject)
        -> PyResult<PyList>
    {
        let mut list: Vec<usize> = offsets.extract(py)?;
        let data: &[u8] = data.data(py);
        list.sort();
        let result = InflatedPos::from_offsets(data, &list)
            .map_err(|e| PyErr::new::<RuntimeError, _>(py, e.to_string()))?;
        Ok(result.into_iter()
            .map(|pos| SourcePoint::create_instance(py, pos))
            .collect::<Result<Vec<_>, _>>()?
            .to_py_object(py))
    }
    @property def line(&self) -> PyResult<PyInt> {
        Ok((self._position(py).line + 1).to_py_object(py))
    }
    @property def zero_based_line(&self) -> PyResult<PyInt> {
        Ok((self._position(py).line).to_py_object(py))
    }
    @property def column(&self) -> PyResult<PyInt> {
        Ok((self._position(py).column + 1).to_py_object(py))
    }
    @property def utf16column(&self) -> PyResult<PyInt> {
        Ok((self._position(py).utf16column).to_py_object(py))
    }
    @property def offset(&self) -> PyResult<PyInt> {
        Ok((self._position(py).offset).to_py_object(py))
    }
    @property def char_offset(&self) -> PyResult<PyInt> {
        Ok((self._position(py).char_offset).to_py_object(py))
    }
});
