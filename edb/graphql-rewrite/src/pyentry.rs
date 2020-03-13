use cpython::{Python, PyClone, ToPyObject, PythonObject, ObjectProtocol};
use cpython::{PyString, PyResult, PyTuple, PyDict, PyList, PyObject};

use edb_graphql_parser::common::{unquote_string, unquote_block_string};
use edb_graphql_parser::position::Pos;

use crate::pyerrors::{LexingError, SyntaxError, NotFoundError, AssertionError};
use crate::entry_point::{Value, Error};
use crate::pytoken::PyToken;
use crate::entry_point;


py_class!(pub class Entry |py| {
    data _key: PyString;
    data _variables: PyDict;
    data _substitutions: PyDict;
    data _tokens: Vec<PyToken>;
    data _end_pos: Pos;
    def key(&self) -> PyResult<PyString> {
        Ok(self._key(py).clone_ref(py))
    }
    def variables(&self) -> PyResult<PyDict> {
        Ok(self._variables(py).clone_ref(py))
    }
    def substitutions(&self) -> PyResult<PyDict> {
        Ok(self._substitutions(py).clone_ref(py))
    }
    def tokens(&self, kinds: PyObject) -> PyResult<PyList> {
        use crate::pytoken::PyTokenKind as K;

        let sof = kinds.get_item(py, "SOF")?;
        let eof = kinds.get_item(py, "EOF")?;
        let bang = kinds.get_item(py, "BANG")?;
        let bang_v = "!".to_py_object(py).into_object();
        let dollar = kinds.get_item(py, "DOLLAR")?;
        let dollar_v = "$".to_py_object(py).into_object();
        let paren_l = kinds.get_item(py, "PAREN_L")?;
        let paren_l_v = "(".to_py_object(py).into_object();
        let paren_r = kinds.get_item(py, "PAREN_R")?;
        let paren_r_v = ")".to_py_object(py).into_object();
        let spread = kinds.get_item(py, "SPREAD")?;
        let spread_v = "...".to_py_object(py).into_object();
        let colon = kinds.get_item(py, "COLON")?;
        let colon_v = ":".to_py_object(py).into_object();
        let equals = kinds.get_item(py, "EQUALS")?;
        let equals_v = "=".to_py_object(py).into_object();
        let at = kinds.get_item(py, "AT")?;
        let at_v = "@".to_py_object(py).into_object();
        let bracket_l = kinds.get_item(py, "BRACKET_L")?;
        let bracket_l_v = "[".to_py_object(py).into_object();
        let bracket_r = kinds.get_item(py, "BRACKET_R")?;
        let bracket_r_v = "]".to_py_object(py).into_object();
        let brace_l = kinds.get_item(py, "BRACE_L")?;
        let brace_l_v = "{".to_py_object(py).into_object();
        let pipe = kinds.get_item(py, "PIPE")?;
        let pipe_v = "|".to_py_object(py).into_object();
        let brace_r = kinds.get_item(py, "BRACE_R")?;
        let brace_r_v = "}".to_py_object(py).into_object();
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
            let (kind, value) = match el.kind {
                K::Sof => (sof.clone_ref(py), py.None()),
                K::Eof => (eof.clone_ref(py), py.None()),
                K::Bang => (bang.clone_ref(py), bang_v.clone_ref(py)),
                K::Dollar => (dollar.clone_ref(py), dollar_v.clone_ref(py)),
                K::ParenL => (paren_l.clone_ref(py), paren_l_v.clone_ref(py)),
                K::ParenR => (paren_r.clone_ref(py), paren_r_v.clone_ref(py)),
                K::Spread => (spread.clone_ref(py), spread_v.clone_ref(py)),
                K::Colon => (colon.clone_ref(py), colon_v.clone_ref(py)),
                K::Equals => (equals.clone_ref(py), equals_v.clone_ref(py)),
                K::At => (at.clone_ref(py), at_v.clone_ref(py)),
                K::BracketL => (bracket_l.clone_ref(py),
                                bracket_l_v.clone_ref(py)),
                K::BracketR => (bracket_r.clone_ref(py),
                                bracket_r_v.clone_ref(py)),
                K::BraceL => (brace_l.clone_ref(py), brace_l_v.clone_ref(py)),
                K::Pipe => (pipe.clone_ref(py), pipe_v.clone_ref(py)),
                K::BraceR => (brace_r.clone_ref(py), brace_r_v.clone_ref(py)),
                K::Name => (name.clone_ref(py),
                            el.value.to_py_object(py).into_object()),
                K::Int => (int.clone_ref(py),
                           el.value.to_py_object(py).into_object()),
                K::Float => (float.clone_ref(py),
                           el.value.to_py_object(py).into_object()),
                K::String => {
                    // graphql-core 3 receives unescaped strings from the lexer
                    let v = unquote_string(&el.value)
                        .map_err(|e| LexingError::new(py, e.to_string()))?
                        .to_py_object(py).into_object();
                    (string.clone_ref(py), v)
                }
                K::BlockString => {
                    // graphql-core 3 receives unescaped strings from the lexer
                    let v = unquote_block_string(&el.value)
                        .map_err(|e| LexingError::new(py, e.to_string()))?
                        .to_py_object(py).into_object();
                    (block_string.clone_ref(py), v)
                }
            };
            elems.push(PyTuple::new(py, &[
                kind,
                el.position.map(|x| x.character)
                    .to_py_object(py).into_object(),
                el.position.map(|x| x.character + el.value.chars().count())
                    .to_py_object(py).into_object(),
                el.position.map(|x| x.line)
                    .to_py_object(py).into_object(),
                el.position.map(|x| x.column)
                    .to_py_object(py).into_object(),
                value,
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
            let substitutions = PyDict::new(py);
            for (idx, var) in entry.variables.iter().enumerate() {
                let s = format!("_edb_arg__{}", idx).to_py_object(py);
                vars.set_item(py, s.clone_ref(py),
                    match var.value {
                        Value::Str(ref s) => PyString::new(py, s),
                        _ => todo!(),
                    })?;
                substitutions.set_item(py, s.clone_ref(py), (
                    &var.token.value,
                    var.token.position.map(|x| x.line),
                    var.token.position.map(|x| x.column),
                ).to_py_object(py).into_object())?;
            }
            // TODO(tailhook) insert defaults
            Entry::create_instance(py,
                PyString::new(py, &entry.key),
                vars,
                substitutions,
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
