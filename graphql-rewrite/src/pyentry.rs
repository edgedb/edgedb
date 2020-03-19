use cpython::{Python, PyClone, ToPyObject, PythonObject, ObjectProtocol};
use cpython::{PyString, PyResult, PyTuple, PyDict, PyList, PyObject};

use crate::position::Pos;
use crate::pyerrors::{LexingError, SyntaxError, NotFoundError, AssertionError};
use crate::entry_point::{Variable, Error};
use crate::pytoken::{PyToken, PyTokenKind};
use crate::entry_point;


py_class!(pub class Entry |py| {
    data _key: PyString;
    data _variables: PyDict;
    data _tokens: Vec<PyToken>;
    data _end_pos: Pos;
    def key(&self) -> PyResult<PyString> {
        Ok(self._key(py).clone_ref(py))
    }
    def variables(&self) -> PyResult<PyDict> {
        Ok(self._variables(py).clone_ref(py))
    }
    def tokens(&self, kinds: PyObject) -> PyResult<PyList> {

        let sof = kinds.get_item(py, "SOF")?;
        let eof = kinds.get_item(py, "EOF")?;
        let bang = kinds.get_item(py, "BANG")?;
        let dollar = kinds.get_item(py, "DOLLAR")?;
        let paren_l = kinds.get_item(py, "PAREN_L")?;
        let paren_r = kinds.get_item(py, "PAREN_R")?;
        let spread = kinds.get_item(py, "SPREAD")?;
        let colon = kinds.get_item(py, "COLON")?;
        let equals = kinds.get_item(py, "EQUALS")?;
        let at = kinds.get_item(py, "AT")?;
        let bracket_l = kinds.get_item(py, "BRACKET_L")?;
        let bracket_r = kinds.get_item(py, "BRACKET_R")?;
        let brace_l = kinds.get_item(py, "BRACE_L")?;
        let pipe = kinds.get_item(py, "PIPE")?;
        let brace_r = kinds.get_item(py, "BRACE_R")?;
        let name = kinds.get_item(py, "NAME")?;
        let int = kinds.get_item(py, "INT")?;
        let float = kinds.get_item(py, "FLOAT")?;
        let string = kinds.get_item(py, "STRING")?;
        let block_string = kinds.get_item(py, "BLOCK_STRING")?;

        let tokens = self._tokens(py);
        let mut elems = Vec::with_capacity(tokens.len());
        elems.push(PyTuple::new(py, &[
            sof.clone_ref(py),
            0u32.to_py_object(py).into_object(),
            0u32.to_py_object(py).into_object(),
            0u32.to_py_object(py).into_object(),
            0u32.to_py_object(py).into_object(),
            py.None(),
        ]).into_object());
        for el in tokens {
            elems.push(PyTuple::new(py, &[
                match el.kind {
                    PyTokenKind::Sof => sof.clone_ref(py),
                    PyTokenKind::Eof => eof.clone_ref(py),
                    PyTokenKind::Bang => bang.clone_ref(py),
                    PyTokenKind::Dollar => dollar.clone_ref(py),
                    PyTokenKind::ParenL => paren_l.clone_ref(py),
                    PyTokenKind::ParenR => paren_r.clone_ref(py),
                    PyTokenKind::Spread => spread.clone_ref(py),
                    PyTokenKind::Colon => colon.clone_ref(py),
                    PyTokenKind::Equals => equals.clone_ref(py),
                    PyTokenKind::At => at.clone_ref(py),
                    PyTokenKind::BracketL => bracket_l.clone_ref(py),
                    PyTokenKind::BracketR => bracket_r.clone_ref(py),
                    PyTokenKind::BraceL => brace_l.clone_ref(py),
                    PyTokenKind::Pipe => pipe.clone_ref(py),
                    PyTokenKind::BraceR => brace_r.clone_ref(py),
                    PyTokenKind::Name => name.clone_ref(py),
                    PyTokenKind::Int => int.clone_ref(py),
                    PyTokenKind::Float => float.clone_ref(py),
                    PyTokenKind::String => string.clone_ref(py),
                    PyTokenKind::BlockString => block_string.clone_ref(py),
                },
                el.position.map(|x| x.character)
                    .to_py_object(py).into_object(),
                el.position.map(|x| x.character + el.value.chars().count())
                    .to_py_object(py).into_object(),
                el.position.map(|x| x.line)
                    .to_py_object(py).into_object(),
                el.position.map(|x| x.column)
                    .to_py_object(py).into_object(),
                el.value.to_py_object(py).into_object(),
            ]).into_object());
        }
        let pos = self._end_pos(py);
        let end_off = pos.character.to_py_object(py).into_object();
        elems.push(PyTuple::new(py, &[
            eof.clone_ref(py),
            end_off.clone_ref(py),
            pos.line.to_py_object(py).into_object(),
            pos.column.to_py_object(py).into_object(),
            end_off,
            py.None(),
        ]).into_object());
        Ok(PyList::new(py, &elems[..]))
    }
});

fn init_module(_py: Python<'_>) {
}

fn rewrite(py: Python<'_>, operation: Option<&PyString>, text: &PyString)
    -> PyResult<Entry>
{
    let oper = operation.map(|x| x.to_string(py)).transpose()?;
    let text = text.to_string(py)?;
    match entry_point::rewrite(oper.as_ref().map(|x| &x[..]), &text) {
        Ok(entry) => {
            let vars = PyDict::new(py);
            for (idx, var) in entry.variables.iter().enumerate() {
                vars.set_item(py,
                    format!("_edb_arg__{}", idx).to_py_object(py),
                    match var {
                        Variable::Str(s) => PyString::new(py, s),
                        _ => todo!(),
                    })?;
            }
            // TODO(tailhook) insert defaults
            Entry::create_instance(py,
                PyString::new(py, &entry.key),
                vars,
                entry.tokens,
                entry.end_pos,
            )
        }
        Err(Error::Lexing(e)) => Err(LexingError::new(py, e.to_string())),
        Err(Error::Syntax(e)) => Err(SyntaxError::new(py, e.to_string())),
        Err(Error::NotFound(e)) => Err(NotFoundError::new(py, e.to_string())),
        Err(Error::Assertion(e))
        => Err(AssertionError::new(py, e.to_string())),
    }
}

py_module_initializer!(
    _graphql_rewrite, init_graphql_rewrite, PyInit__graphql_rewrite,
    |py, m| {
        init_module(py);
        m.add(py, "__doc__", "Rust optimizer for graphql queries")?;
        m.add(py, "rewrite",
            py_fn!(py, rewrite(option: Option<&PyString>, data: &PyString)))?;
        m.add(py, "Entry", py.get_type::<Entry>())?;
        m.add(py, "LexingError", py.get_type::<LexingError>())?;
        m.add(py, "SyntaxError", py.get_type::<SyntaxError>())?;
        m.add(py, "NotFoundError", py.get_type::<NotFoundError>())?;
        m.add(py, "AssertionError", py.get_type::<AssertionError>())?;
        Ok(())
    });
