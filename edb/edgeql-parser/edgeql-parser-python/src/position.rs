use pyo3::{
    exceptions::{PyIndexError, PyRuntimeError},
    prelude::*,
    types::{PyBytes, PyList},
};

use edgeql_parser::position::InflatedPos;

#[pyclass]
pub struct SourcePoint {
    _position: InflatedPos,
}

#[pymethods]
impl SourcePoint {
    #[staticmethod]
    fn from_offsets(py: Python, data: &Bound<PyBytes>, offsets: PyObject) -> PyResult<Py<PyList>> {
        let mut list: Vec<usize> = offsets.extract(py)?;
        let data: &[u8] = data.as_bytes();
        list.sort();
        let result = InflatedPos::from_offsets(data, &list)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        PyList::new(
            py,
            result
                .into_iter()
                .map(|_position| SourcePoint { _position }),
        )
        .map(|v| v.into())
    }

    #[getter]
    fn line(&self) -> u64 {
        self._position.line + 1
    }
    #[getter]
    fn zero_based_line(&self) -> u64 {
        self._position.line
    }
    #[getter]
    fn column(&self) -> u64 {
        self._position.column + 1
    }
    #[getter]
    fn utf16column(&self) -> u64 {
        self._position.utf16column
    }
    #[getter]
    fn offset(&self) -> u64 {
        self._position.offset
    }
    #[getter]
    fn char_offset(&self) -> u64 {
        self._position.char_offset
    }
}

fn _offset_of_line(text: &str, target: usize) -> Option<usize> {
    let mut was_lf = false;
    let mut line = 0; // this assumes line found by rfind
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

#[pyfunction]
pub fn offset_of_line(text: &str, target: usize) -> PyResult<usize> {
    match _offset_of_line(text, target) {
        Some(offset) => Ok(offset),
        None => Err(PyIndexError::new_err("line number is too large")),
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
