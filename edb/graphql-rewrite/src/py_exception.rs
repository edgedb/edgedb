use pyo3::{create_exception, exceptions::PyException, PyErr};

use crate::rewrite::Error;

create_exception!(_graphql_rewrite, LexingError, PyException);

create_exception!(_graphql_rewrite, SyntaxError, PyException);

create_exception!(_graphql_rewrite, NotFoundError, PyException);

create_exception!(_graphql_rewrite, AssertionError, PyException);

create_exception!(_graphql_rewrite, QueryError, PyException);

pub fn convert_error(error: Error) -> PyErr {
    match error {
        Error::Lexing(e) => LexingError::new_err(e),
        Error::Syntax(e) => SyntaxError::new_err(e.to_string()),
        Error::NotFound(e) => NotFoundError::new_err(e),
        Error::Query(e) => QueryError::new_err(e),
        Error::Assertion(e) => AssertionError::new_err(e),
    }
}
