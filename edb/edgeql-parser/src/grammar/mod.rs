#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
// temporary
#![allow(dead_code)]

mod from_id;
mod non_terminals;
mod stub;

use non_terminals::*;
use stub::*;

use crate::parser::CSTNode;

pub fn cst_to_ast(node: &CSTNode) -> TodoAst {
    EdgeQLGrammar::reduce(node)
}

trait Reduce {
    type Output;

    fn reduce(node: &CSTNode) -> Self::Output;
}

trait FromId: Sized {
    fn from_id(id: usize) -> Self;
}

#[derive(Debug)]
pub struct TodoAst;
