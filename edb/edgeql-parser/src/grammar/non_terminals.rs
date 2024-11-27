use super::{FromId, Reduce};
use crate::ast;
use crate::parser::CSTNode;

#[derive(edgeql_parser_derive::Reduce)]
#[output(Vec::<ast::AccessKind>)]
pub enum AccessKind {
    ALL,
    DELETE,
    INSERT,
    SELECT,
    UPDATE,
    UPDATE_READ,
    UPDATE_WRITE,
}

impl From<AccessKindData> for Vec<ast::AccessKind> {
    fn from(value: AccessKindData) -> Self {
        use ast::AccessKind::*;
        match value {
            AccessKindData::ALL() => vec![Delete, Insert, Select, UpdateRead, UpdateWrite],
            AccessKindData::DELETE() => vec![Delete],
            AccessKindData::INSERT() => vec![Insert],
            AccessKindData::SELECT() => vec![Select],
            AccessKindData::UPDATE() => {
                vec![UpdateRead, UpdateWrite]
            }
            AccessKindData::UPDATE_READ() => vec![UpdateRead],
            AccessKindData::UPDATE_WRITE() => vec![UpdateWrite],
        }
    }
}

macro_rules! list {
    ($name: ident, $inner: ident) => {
        #[derive(edgeql_parser_derive::Reduce)]
        #[output(Vec::<Vec::<ast::AccessKind>>)]
        pub enum $name {
            $inner,
            AccessKindList_COMMA_AccessKind,
        }

        impl From<AccessKindListData> for Vec<Vec<ast::AccessKind>> {
            fn from(value: AccessKindListData) -> Self {
                match value {
                    AccessKindListData::AccessKind(a) => {
                        vec![a]
                    }
                    AccessKindListData::AccessKindList_COMMA_AccessKind(mut list, a) => {
                        list.push(a);
                        list
                    }
                }
            }
        }
    };
}

list!(AccessKindList, AccessKind);
