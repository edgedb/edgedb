
use cpython::exc::{RuntimeError, IndexError};
use cpython::{py_class, PyErr, PyResult, PyInt, PyBytes, PyList, ToPyObject};
use cpython::{Python, PyObject};

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

fn _offset_of_line(text: &str, target: usize) -> Option<usize> {
    let mut was_lf = false;
    let mut line = 0;  // this assumes line found by rfind
    for (idx, &byte) in text.as_bytes().iter().enumerate() {
        if line >= target {
            return Some(idx);
        }
        match byte {
            b'\n' => {
                line += 1;
                was_lf = false;
            }
            _ if was_lf => {
                line += 1;
                if line >= target {
                    return Some(idx);
                }
                was_lf = byte == b'\r';
            }
            b'\r' => {
                was_lf = true;
            }
            _ => {}
        }
    }
    if was_lf {
        line += 1;
    }
    if target > line {
        return None;
    }
    Some(text.len())
}

pub fn offset_of_line(py: Python, text: &str, target: usize) -> PyResult<usize>
{
    match _offset_of_line(text, target) {
        Some(offset) => Ok(offset),
        None => {
            Err(PyErr::new::<IndexError, _>(py, "line number is too large"))
        }
    }
}

#[test]
fn line_offsets() {
    assert_eq!(_offset_of_line("line1\nline2\nline3", 0), Some(0));
    assert_eq!(_offset_of_line("line1\nline2\nline3", 1), Some(6));
    assert_eq!(_offset_of_line("line1\nline2\nline3", 2), Some(12));
    assert_eq!(_offset_of_line("line1\nline2\nline3", 3), None);
    assert_eq!(_offset_of_line("line1\rline2\rline3", 0), Some(0));
    assert_eq!(_offset_of_line("line1\rline2\rline3", 1), Some(6));
    assert_eq!(_offset_of_line("line1\rline2\rline3", 2), Some(12));
    assert_eq!(_offset_of_line("line1\rline2\rline3", 3), None);
    assert_eq!(_offset_of_line("line1\r\nline2\r\nline3", 0), Some(0));
    assert_eq!(_offset_of_line("line1\r\nline2\r\nline3", 1), Some(7));
    assert_eq!(_offset_of_line("line1\r\nline2\r\nline3", 2), Some(14));
    assert_eq!(_offset_of_line("line1\r\nline2\r\nline3", 3), None);
    assert_eq!(_offset_of_line("line1\rline2\r\nline3\n", 0), Some(0));
    assert_eq!(_offset_of_line("line1\rline2\r\nline3\n", 1), Some(6));
    assert_eq!(_offset_of_line("line1\rline2\r\nline3\n", 2), Some(13));
    assert_eq!(_offset_of_line("line1\rline2\r\nline3\n", 3), Some(19));
    assert_eq!(_offset_of_line("line1\rline2\r\nline3\n", 4), None);
    assert_eq!(_offset_of_line("line1\nline2\rline3\r\n", 0), Some(0));
    assert_eq!(_offset_of_line("line1\nline2\rline3\r\n", 1), Some(6));
    assert_eq!(_offset_of_line("line1\nline2\rline3\r\n", 2), Some(12));
    assert_eq!(_offset_of_line("line1\nline2\rline3\r\n", 3), Some(19));
    assert_eq!(_offset_of_line("line1\nline2\rline3\r\n", 4), None);
    assert_eq!(_offset_of_line("line1\n\rline2\r\rline3\r", 0), Some(0));
    assert_eq!(_offset_of_line("line1\n\rline2\r\rline3\r", 1), Some(6));
    assert_eq!(_offset_of_line("line1\n\rline2\r\rline3\r", 2), Some(7));
    assert_eq!(_offset_of_line("line1\n\rline2\r\rline3\r", 3), Some(13));
    assert_eq!(_offset_of_line("line1\n\rline2\r\rline3\r", 4), Some(14));
    assert_eq!(_offset_of_line("line1\n\rline2\r\rline3\r", 5), Some(20));
    assert_eq!(_offset_of_line("line1\n\rline2\r\rline3\r", 6), None);
}
