use super::*;
use crate::ast;
use crate::parser::CSTNode;

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::GrammarEntryPoint)]
pub enum EdgeQLGrammar {
    STARTBLOCK_EdgeQLBlock_EOI,
    STARTEXTENSION_CreateExtensionPackageCommandsBlock_EOI,
    STARTFRAGMENT_ExprStmt_EOI,
    STARTFRAGMENT_Expr_EOI,
    STARTMIGRATION_CreateMigrationCommandsBlock_EOI,
    STARTSDLDOCUMENT_SDLDocument_EOI,
}

impl Into<ast::GrammarEntryPoint> for EdgeQLGrammarNode {
    fn into(self) -> ast::GrammarEntryPoint {
        match self {
            Self::STARTBLOCK_EdgeQLBlock_EOI(_) => todo!(),
            Self::STARTEXTENSION_CreateExtensionPackageCommandsBlock_EOI(_) => todo!(),
            Self::STARTFRAGMENT_ExprStmt_EOI(x) => ast::GrammarEntryPoint::Query(x),
            Self::STARTFRAGMENT_Expr_EOI(x) => ast::GrammarEntryPoint::Expr(x),
            Self::STARTMIGRATION_CreateMigrationCommandsBlock_EOI(_) => todo!(),
            Self::STARTSDLDOCUMENT_SDLDocument_EOI(_) => todo!(),
        }
    }
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(Box::<ast::Expr>)]
#[stub()]
pub enum Expr {
    BaseAtomicExpr,
    DETACHED_Expr,
    DISTINCT_Expr,
    EXISTS_Expr,
    Expr_AND_Expr,
    Expr_CIRCUMFLEX_Expr,
    Expr_CompareOp_Expr_P_COMPARE_OP,
    Expr_DOUBLEPLUS_Expr,
    Expr_DOUBLEQMARK_Expr_P_DOUBLEQMARK_OP,
    Expr_DOUBLESLASH_Expr,
    Expr_EXCEPT_Expr,
    Expr_IF_Expr_ELSE_Expr,
    Expr_ILIKE_Expr,
    Expr_INTERSECT_Expr,
    Expr_IN_Expr,
    Expr_IS_NOT_TypeExpr_P_IS,
    Expr_IS_TypeExpr,
    Expr_IndirectionEl,
    Expr_LIKE_Expr,
    Expr_MINUS_Expr,
    Expr_NOT_ILIKE_Expr,
    Expr_NOT_IN_Expr_P_IN,
    Expr_NOT_LIKE_Expr,
    Expr_OR_Expr,
    Expr_PERCENT_Expr,
    Expr_PLUS_Expr,
    Expr_SLASH_Expr,
    Expr_STAR_Expr,
    Expr_Shape,
    Expr_UNION_Expr,
    GLOBAL_NodeName,
    INTROSPECT_TypeExpr,
    IfThenElseExpr,
    LANGBRACKET_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
    LANGBRACKET_OPTIONAL_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
    LANGBRACKET_REQUIRED_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
    MINUS_Expr_P_UMINUS,
    NOT_Expr,
    PLUS_Expr_P_UMINUS,
    Path,
}

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

impl From<AccessKindNode> for Vec<ast::AccessKind> {
    fn from(value: AccessKindNode) -> Self {
        use ast::AccessKind::*;
        match value {
            AccessKindNode::ALL() => vec![Delete, Insert, Select, UpdateRead, UpdateWrite],
            AccessKindNode::DELETE() => vec![Delete],
            AccessKindNode::INSERT() => vec![Insert],
            AccessKindNode::SELECT() => vec![Select],
            AccessKindNode::UPDATE() => {
                vec![UpdateRead, UpdateWrite]
            }
            AccessKindNode::UPDATE_READ() => vec![UpdateRead],
            AccessKindNode::UPDATE_WRITE() => vec![UpdateWrite],
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

        impl From<AccessKindListNode> for Vec<Vec<ast::AccessKind>> {
            fn from(value: AccessKindListNode) -> Self {
                match value {
                    AccessKindListNode::AccessKind(a) => {
                        vec![a]
                    }
                    AccessKindListNode::AccessKindList_COMMA_AccessKind(mut list, a) => {
                        list.push(a);
                        list
                    }
                }
            }
        }
    };
}

list!(AccessKindList, AccessKind);
