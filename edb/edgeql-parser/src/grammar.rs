#![allow(non_snake_case)]

use crate::ast;
use crate::reductions;
use crate::parser::{CSTNode, Production};

fn unpack_prod<'a>(node: &'a CSTNode<'a>) -> &'a Production<'a> {
    match node {
        CSTNode::Production(p) => p,
        _ => unreachable!(),
    }
}

fn unpack_args<'a, const A: usize>(node: &'a CSTNode<'a>) -> &'a [CSTNode<'a>; A] {
    unpack_prod(node).args.try_into().unwrap()
}

fn unpack_prod_reduction<'a>(node: &'a CSTNode<'a>) -> reductions::Reduction {
    let id = unpack_prod(node).id;
    reductions::reduction_from_id(id)
}

fn expr_todo() -> Box<ast::Expr> {
    Box::new(ast::Expr::Anchor(ast::Anchor::SpecialAnchor(
        ast::SpecialAnchor {
            name: "__todo__".into(),
        },
    )))
}

mod Expr {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Box<ast::Expr> {
        todo!()
    }
}

mod OptDirection {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Option<ast::SortOrder> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OptDirection(reductions::OptDirection::ASC) => reduce_ASC(node),
            reductions::Reduction::OptDirection(reductions::OptDirection::DESC) => reduce_DESC(node),
            reductions::Reduction::OptDirection(reductions::OptDirection::epsilon) => reduce_empty(node),
            _ => unreachable!(),
        }
    }

    fn reduce_ASC(_: &CSTNode) -> Option<ast::SortOrder> {
        Some(ast::SortOrder::Asc)
    }

    fn reduce_DESC(_: &CSTNode) -> Option<ast::SortOrder> {
        Some(ast::SortOrder::Desc)
    }

    fn reduce_empty(_: &CSTNode) -> Option<ast::SortOrder> {
        None
    }
}

mod OptNonesOrder {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Option<ast::NonesOrder> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OptNonesOrder(reductions::OptNonesOrder::EMPTY_FIRST) => reduce_EMPTY_FIRST(node),
            reductions::Reduction::OptNonesOrder(reductions::OptNonesOrder::EMPTY_LAST) => reduce_EMPTY_LAST(node),
            reductions::Reduction::OptNonesOrder(reductions::OptNonesOrder::epsilon) => reduce_empty(node),
            _ => unreachable!(),
        }
    }

    fn reduce_EMPTY_FIRST(_: &CSTNode) -> Option<ast::NonesOrder> {
        Some(ast::NonesOrder::First)
    }

    fn reduce_EMPTY_LAST(_: &CSTNode) -> Option<ast::NonesOrder> {
        Some(ast::NonesOrder::Last)
    }

    fn reduce_empty(_: &CSTNode) -> Option<ast::NonesOrder> {
        None
    }
}

mod OrderbyExpr {
    use super::*;

    pub fn reduce(node: &CSTNode) -> ast::SortExpr {
        reduce_Expr_OptDirection_OptNonesOrder(node)
    }

    fn reduce_Expr_OptDirection_OptNonesOrder(node: &CSTNode) -> ast::SortExpr {
        let [expr, opt_direction, opt_nones_order] = unpack_args(node);
        ast::SortExpr {
            path: super::Expr::reduce(expr),
            direction: super::OptDirection::reduce(opt_direction),
            nones_order: super::OptNonesOrder::reduce(opt_nones_order),
        }
    }
}

/// (ListNonterm, element=OrderbyExpr, separator=tokens.T_THEN)
mod OrderbyList {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Vec<ast::SortExpr> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OrderbyList(reductions::OrderbyList::OrderbyExpr) => reduce_OrderbyExpr(node),
            reductions::Reduction::OrderbyList(reductions::OrderbyList::OrderbyList_THEN_OrderbyExpr) => reduce_OrderbyList_THEN_OrderbyExpr(node),
            _ => unreachable!(),
        }
    }

    fn reduce_OrderbyExpr(node: &CSTNode) -> Vec<ast::SortExpr> {
        let [orderby_expr] = unpack_args(node);
        vec![OrderbyExpr::reduce(orderby_expr)]
    }

    fn reduce_OrderbyList_THEN_OrderbyExpr(node: &CSTNode) -> Vec<ast::SortExpr> {
        let [orderby_list, _then, orderby_expr] = unpack_args(node);
        let mut res = OrderbyList::reduce(orderby_list);
        res.push(OrderbyExpr::reduce(orderby_expr));
        res
    }
}

mod SortClause {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Vec<ast::SortExpr> {
        reduce_ORDERBY_OrderbyList(node)
    }

    /// @parsing.inline(1)
    fn reduce_ORDERBY_OrderbyList(node: &CSTNode) -> Vec<ast::SortExpr> {
        let [_orderby, orderby_list] = unpack_args(node);
        OrderbyList::reduce(orderby_list)
    }
}

mod OptSortClause {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Vec<ast::SortExpr> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OptSortClause(reductions::OptSortClause::SortClause) => reduce_SortClause(node),
            reductions::Reduction::OptSortClause(reductions::OptSortClause::epsilon) => reduce_empty(node),
            _ => unreachable!(),
        }
    }

    /// @parsing.inline(0)
    fn reduce_SortClause(node: &CSTNode) -> Vec<ast::SortExpr> {
        let [sort_clause] = unpack_args(node);
        SortClause::reduce(sort_clause)
    }

    fn reduce_empty(node: &CSTNode) -> Vec<ast::SortExpr> {
        vec![]
    }
}

mod SimpleSelect {
    use super::*;

    pub fn reduce(node: &CSTNode) -> ast::SelectQuery {
        match unpack_prod_reduction(node) {
            reductions::Reduction::SimpleSelect(_) => reduce_Select(node),
            _ => unreachable!(),
        }
    }

    /// %reduce SELECT OptionallyAliasedExpr OptFilterClause OptSortClause OptSelectLimit
    fn reduce_Select(node: &CSTNode) -> ast::SelectQuery {
        let [_SELECT, OptionallyAliasedExpr, OptFilterClause, OptSortClause, OptSelectLimit] =
            unpack_args(node);

        let result = (None, expr_todo()); // OptionallyAliasedExpr::reduce(OptionallyAliasedExpr);
        let r#where = None; // OptFilterClause::reduce(OptFilterClause);
        let orderby = OptSortClause::reduce(OptSortClause);
        let (offset, limit) = (None, None); // OptSelectLimit::reduce(OptSelectLimit);

        if offset.is_some() || limit.is_some() {
            let subj = ast::SelectQuery {
                aliases: None,
                result_alias: result.0,
                result: result.1,
                r#where,
                orderby: Some(orderby),
                offset: None,
                limit: None,
                rptr_passthrough: false,
                implicit: true,
            };
            let subj = Box::new(ast::Expr::Query(ast::Query::SelectQuery(subj)));

            ast::SelectQuery {
                aliases: None,
                result_alias: None,
                result: subj,
                r#where: None,
                orderby: None,
                offset,
                limit,
                rptr_passthrough: false,
                implicit: false,
            }
        } else {
            ast::SelectQuery {
                aliases: None,
                result_alias: result.0,
                result: result.1,
                r#where,
                orderby: Some(orderby),
                offset,
                limit,
                rptr_passthrough: false,
                implicit: false,
            }
        }
    }
}

mod ExprStmtCore {
    use super::*;

    pub fn reduce(node: &CSTNode) -> ast::Query {
        match unpack_prod_reduction(node) {
            reductions::Reduction::ExprStmtCore(reductions::ExprStmtCore::InternalGroup) => reduce_InternalGroup(node),
            reductions::Reduction::ExprStmtCore(reductions::ExprStmtCore::SimpleDelete) => reduce_SimpleDelete(node),
            reductions::Reduction::ExprStmtCore(reductions::ExprStmtCore::SimpleFor) => reduce_SimpleFor(node),
            reductions::Reduction::ExprStmtCore(reductions::ExprStmtCore::SimpleGroup) => reduce_SimpleGroup(node),
            reductions::Reduction::ExprStmtCore(reductions::ExprStmtCore::SimpleInsert) => reduce_SimpleInsert(node),
            reductions::Reduction::ExprStmtCore(reductions::ExprStmtCore::SimpleSelect) => reduce_SimpleSelect(node),
            reductions::Reduction::ExprStmtCore(reductions::ExprStmtCore::SimpleUpdate) => reduce_SimpleUpdate(node),
            _ => unreachable!(),
        }
    }

    ///@parsing.inline(0)
    fn reduce_SimpleFor(node: &CSTNode) -> ast::Query {
        todo!()
    }

    ///@parsing.inline(0)
    fn reduce_SimpleSelect(node: &CSTNode) -> ast::Query {
        let [SimpleSelect] =
            unpack_args(node);

        let select = SimpleSelect::reduce(SimpleSelect);
        ast::Query::SelectQuery(select)
    }

    ///@parsing.inline(0)
    fn reduce_SimpleGroup(node: &CSTNode) -> ast::Query {
        todo!()
    }

    ///@parsing.inline(0)
    fn reduce_InternalGroup(node: &CSTNode) -> ast::Query {
        todo!()
    }

    ///@parsing.inline(0)
    fn reduce_SimpleInsert(node: &CSTNode) -> ast::Query {
        todo!()
    }

    ///@parsing.inline(0)
    fn reduce_SimpleUpdate(node: &CSTNode) -> ast::Query {
        todo!()
    }

    ///@parsing.inline(0)
    fn reduce_SimpleDelete(node: &CSTNode) -> ast::Query {
        todo!()
    }
}

mod ExprStmt {
    use super::*;

    pub fn reduce(node: &CSTNode) -> ast::Expr {
        match unpack_prod_reduction(node) {
            reductions::Reduction::ExprStmt(reductions::ExprStmt::ExprStmtCore) => reduce_ExprStmtCore(node),
            reductions::Reduction::ExprStmt(reductions::ExprStmt::WithBlock_ExprStmtCore) => reduce_WithBlock_ExprStmtCore(node),
            _ => unreachable!(),
        }
    }

    fn reduce_WithBlock_ExprStmtCore(node: &CSTNode) -> ast::Expr {
        let [WithBlock, ExprStmtCore] =
            unpack_args(node);

        let aliases = None; // WithBlock::reduce(WithBlock);
        let mut query = ExprStmtCore::reduce(ExprStmtCore);

        match &mut query {
            ast::Query::SelectQuery(q) => q.aliases = aliases,
            ast::Query::GroupQuery(q) => q.aliases = aliases,
            ast::Query::InternalGroupQuery(q) => q.aliases = aliases,
            ast::Query::InsertQuery(q) => q.aliases = aliases,
            ast::Query::UpdateQuery(q) => q.aliases = aliases,
            ast::Query::DeleteQuery(q) => q.aliases = aliases,
            ast::Query::ForQuery(q) => q.aliases = aliases,
        }

        ast::Expr::Query(query)
    }

    ///@parsing.inline(0)
    fn reduce_ExprStmtCore(node: &CSTNode) -> ast::Expr {
        let [ExprStmtCore] = unpack_args(node);
        let query = ExprStmtCore::reduce(ExprStmtCore);
        ast::Expr::Query(query)
    }
}
