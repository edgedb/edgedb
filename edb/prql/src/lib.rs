#[macro_use]
extern crate cpython;

use cpython::exc::ValueError;
use cpython::{PyErr, PyObject, PyResult, PyString, Python, PythonObject, ToPyObject};

py_module_initializer!(_prql, init_prql, PyInit__prql, |py, m| {
    m.add(py, "compile", py_fn!(py, compile(query: &PyString)))?;
    Ok(())
});

fn compile(py: Python, query: &PyString) -> PyResult<PyObject> {
    let options = prql_compiler::Options::default()
        .with_signature_comment(false)
        .with_format(false)
        .with_target(prql_compiler::Target::Sql(Some(
            prql_compiler::sql::Dialect::Postgres,
        )));

    let query = query.to_string(py).unwrap();

    let res = prql_compiler::compile(&query, &options);

    match res {
        Ok(sql_string) => Ok(sql_string.into_py_object(py).into_object()),
        Err(messages) => {
            let messages_json = serde_json::to_string(&messages).unwrap();

            Err(PyErr::new_lazy_init(
                py.get_type::<ValueError>(),
                Some(messages_json.to_py_object(py).into_object()),
            ))
        }
    }
}
