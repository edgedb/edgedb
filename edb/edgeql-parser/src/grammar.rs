#![allow(non_snake_case)]

// temporary
#![allow(dead_code)]

use crate::ast;
use crate::reductions;
use crate::parser::{CSTNode, Production, Terminal};
use crate::tokenizer;

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
        let [_select, optionally_aliased_expr, opt_filter_clause, opt_sort_clause, opt_select_limit] =
            unpack_args(node);

        let result = OptionallyAliasedExpr::reduce(optionally_aliased_expr);
        let r#where = OptFilterClause::reduce(opt_filter_clause);
        let orderby = OptSortClause::reduce(opt_sort_clause);
        let (offset, limit) = OptSelectLimit::reduce(opt_select_limit);

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

mod OptionallyAliasedExpr {
    use super::*;

    pub fn reduce(node: &CSTNode) -> (Option<String>, Box<ast::Expr>) {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OptionallyAliasedExpr(reductions::OptionallyAliasedExpr::AliasedExpr) => reduce_AliasedExpr(node),
            reductions::Reduction::OptionallyAliasedExpr(reductions::OptionallyAliasedExpr::Expr) => reduce_Expr(node),
            _ => unreachable!(),
        }
    }

    pub fn reduce_AliasedExpr(node: &CSTNode) -> (Option<String>, Box<ast::Expr>)  {
        let [aliased_expr] = unpack_args(node);
        let aliased_expr_ast = AliasedExpr::reduce(aliased_expr);
        (Some(aliased_expr_ast.alias), aliased_expr_ast.expr)
    }

    pub fn reduce_Expr(node: &CSTNode) -> (Option<String>, Box<ast::Expr>)  {
        (None, Expr::reduce(node))
    }
}

mod AliasedExpr {
    use super::*;

    pub fn reduce(node: &CSTNode) -> ast::AliasedExpr {
        match unpack_prod_reduction(node) {
            reductions::Reduction::AliasedExpr(_) => reduce_AliasedExpr(node),
            _ => unreachable!(),
        }
    }

    /// %reduce Identifier ASSIGN Expr
    fn reduce_AliasedExpr(node: &CSTNode) -> ast::AliasedExpr {
        let [identifier, _assign, expr] = unpack_args(node);

        ast::AliasedExpr {
            alias: Identifier::reduce(identifier),
            expr: Expr::reduce(expr)
        }
    }
}

mod Identifier {
    use super::*;

    pub fn reduce(node: &CSTNode) -> String {
        match unpack_prod_reduction(node) {
            reductions::Reduction::Identifier(reductions::Identifier::IDENT) => reduce_IDENT(node),
            reductions::Reduction::Identifier(reductions::Identifier::UnreservedKeyword) => reduce_UnreservedKeyword(node),
            _ => unreachable!(),
        }
    }

    fn reduce_IDENT(node: &CSTNode) -> String {
        let [ident] = unpack_args(node);

        match ident {
            CSTNode::Terminal(term @ Terminal{kind: tokenizer::Kind::Ident, ..}) => term.text.to_string(),
            _ => unreachable!(),
        }
    }


    /// @parsing.inline(0)
    fn reduce_UnreservedKeyword(node: &CSTNode) -> String {
        UnreservedKeyword::reduce(node)
    }
}

mod UnreservedKeyword {
    use super::*;

    pub fn reduce(node: &CSTNode) -> String {
        match unpack_prod_reduction(node) {
            reductions::Reduction::UnreservedKeyword(_) => reduce_UnreservedKeyword(node),
            _ => unreachable!(),
        }
    }

    fn reduce_UnreservedKeyword(node: &CSTNode) -> String {
        let [kw] = unpack_args(node);

        match kw {
            CSTNode::Terminal(term @ Terminal{kind: tokenizer::Kind::Keyword(_), ..}) => term.text.to_string(),
            _ => unreachable!(),
        }
    }
}

mod OptFilterClause {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Option<Box<ast::Expr>> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OptFilterClause(reductions::OptFilterClause::FilterClause) => reduce_FilterClause(node),
            reductions::Reduction::OptFilterClause(reductions::OptFilterClause::epsilon) => reduce_empty(node),
            _ => unreachable!(),
        }
    }

    /// @parsing.inline(0)
    fn reduce_FilterClause(node: &CSTNode) -> Option<Box<ast::Expr>> {
        let [filter_clause] = unpack_args(node);
        Some(FilterClause::reduce(filter_clause))
    }

    fn reduce_empty(node: &CSTNode) -> Option<Box<ast::Expr>> {
        None
    }
}

mod FilterClause {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Box<ast::Expr> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::FilterClause(_) => reduce_FILTER_Expr(node),
            _ => unreachable!(),
        }
    }

    /// @parsing.inline(1)
    fn reduce_FILTER_Expr(node: &CSTNode) -> Box<ast::Expr> {
        let [_filter, expr] = unpack_args(node);
        Expr::reduce(expr)
    }
}

mod OptSelectLimit {
    use super::*;

    pub fn reduce(node: &CSTNode) -> (Option<Box<ast::Expr>>, Option<Box<ast::Expr>>) {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OptSelectLimit(reductions::OptSelectLimit::SelectLimit) => reduce_SelectLimit(node),
            reductions::Reduction::OptSelectLimit(reductions::OptSelectLimit::epsilon) => reduce_empty(node),
            _ => unreachable!(),
        }
    }

    /// @parsing.inline(0)
    fn reduce_SelectLimit(node: &CSTNode) -> (Option<Box<ast::Expr>>, Option<Box<ast::Expr>>) {
        let [select_limit] = unpack_args(node);
        SelectLimit::reduce(select_limit)
    }

    fn reduce_empty(_node: &CSTNode) -> (Option<Box<ast::Expr>>, Option<Box<ast::Expr>>) {
        (None, None)
    }
}

mod SelectLimit {
    use super::*;

    pub fn reduce(node: &CSTNode) -> (Option<Box<ast::Expr>>, Option<Box<ast::Expr>>) {
        match unpack_prod_reduction(node) {
            reductions::Reduction::SelectLimit(reductions::SelectLimit::OffsetClause_LimitClause) => reduce_OffsetClause_LimitClause(node),
            reductions::Reduction::SelectLimit(reductions::SelectLimit::OffsetClause) => reduce_OffsetClause(node),
            reductions::Reduction::SelectLimit(reductions::SelectLimit::LimitClause) => reduce_LimitClause(node),
            _ => unreachable!(),
        }
    }

    fn reduce_OffsetClause_LimitClause(node: &CSTNode) -> (Option<Box<ast::Expr>>, Option<Box<ast::Expr>>) {
        let [offset_clause, limit_clause] = unpack_args(node);
        (Some(OffsetClause::reduce(offset_clause)), Some(LimitClause::reduce(limit_clause)))
    }

    fn reduce_OffsetClause(node: &CSTNode) -> (Option<Box<ast::Expr>>, Option<Box<ast::Expr>>) {
        let [offset_clause] = unpack_args(node);
        (Some(OffsetClause::reduce(offset_clause)), None)
    }

    fn reduce_LimitClause(node: &CSTNode) -> (Option<Box<ast::Expr>>, Option<Box<ast::Expr>>) {
        let [limit_clause] = unpack_args(node);
        (None, Some(LimitClause::reduce(limit_clause)))
    }
}

mod OffsetClause {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Box<ast::Expr> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::OffsetClause(_) => reduce_OFFSET_Expr(node),
            _ => unreachable!(),
        }
    }

    /// @parsing.inline(1)
    fn reduce_OFFSET_Expr(node: &CSTNode) -> Box<ast::Expr> {
        let [_offset, expr] = unpack_args(node);
        Expr::reduce(expr)
    }
}

mod LimitClause {
    use super::*;

    pub fn reduce(node: &CSTNode) -> Box<ast::Expr> {
        match unpack_prod_reduction(node) {
            reductions::Reduction::LimitClause(_) => reduce_LIMIT_Expr(node),
            _ => unreachable!(),
        }
    }

    /// @parsing.inline(1)
    fn reduce_LIMIT_Expr(node: &CSTNode) -> Box<ast::Expr> {
        let [_offset, expr] = unpack_args(node);
        Expr::reduce(expr)
    }
}
