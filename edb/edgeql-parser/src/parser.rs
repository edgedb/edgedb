use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;

use crate::helpers::quote_name;
use crate::keywords::Keyword;
use crate::position::Span;
use crate::tokenizer::Error;
use crate::tokenizer::Kind;
use crate::tokenizer::Value;

pub fn parse(spec: &Spec, input: Vec<Terminal>) -> (Option<CSTNode>, Vec<Error>) {
    let arena = bumpalo::Bump::new();

    let stack_top = arena.alloc(StackNode {
        parent: None,
        state: 0,
        value: CSTNode::Empty,
    });
    let initial_track = Parser {
        stack_top,
        error_cost: 0,
        node_count: 0,
        can_recover: true,
        errors: Vec::new(),
    };

    let ctx = Context::new(spec, &arena);

    // append EIO
    let end = input.last().map(|t| t.span.end).unwrap_or_default();
    let eio = Terminal {
        kind: Kind::EOI,
        span: Span { start: end, end },
        text: "".to_string(),
        value: None,
    };
    let input = [input, vec![eio]].concat();

    let mut parsers = vec![initial_track];
    let mut prev_span: Option<Span> = None;

    for token in input {
        let mut new_parsers = Vec::with_capacity(parsers.len() + 5);

        while let Some(mut parser) = parsers.pop() {
            let res = parser.act(&ctx, &token);

            if res.is_ok() {
                // base case: ok
                parser.node_successful();
                new_parsers.push(parser);
            } else {
                // error: try to recover

                let gap_span = {
                    let prev_end = prev_span.map(|p| p.end).unwrap_or(token.span.start);

                    Span {
                        start: prev_end,
                        end: token.span.start,
                    }
                };

                // option 1: inject a token
                let possible_actions = &ctx.spec.actions[parser.stack_top.state];
                for token_kind in possible_actions.keys() {
                    let mut inject = parser.clone();

                    let injection = new_token_for_injection(token_kind);

                    let cost = error_cost(token_kind);
                    let error = Error::new(format!("Missing {injection}")).with_span(gap_span);
                    inject.push_error(error, cost);

                    if inject.error_cost <= ERROR_COST_INJECT_MAX {
                        // println!("   --> [inject {injection}]");

                        if inject.act(&ctx, &injection).is_ok() {
                            // insert into parsers, to retry the original token
                            parsers.push(inject);
                        }
                    }
                }

                // option 2: skip the token

                let mut skip = parser;
                let error = Error::new(format!("Unexpected {token}")).with_span(token.span);
                skip.push_error(error, ERROR_COST_SKIP);
                if token.kind == Kind::EOF {
                    // extra penalty
                    skip.error_cost += ERROR_COST_INJECT_MAX;
                    skip.can_recover = false;
                };

                // println!("   --> [skip]");

                // insert into new_parsers, so the token is skipped
                new_parsers.push(skip);
            }
        }

        // has any parser recovered?
        if new_parsers.len() > 1 {
            let recovered = new_parsers.iter().position(Parser::has_recovered);

            if let Some(recovered) = recovered {
                let mut recovered = new_parsers.swap_remove(recovered);
                recovered.error_cost = 0;

                new_parsers.clear();
                new_parsers.push(recovered);
            }
        }

        // prune: pick only X best parsers
        if new_parsers.len() > 10 {
            new_parsers.sort_by_key(Parser::adjusted_cost);
            new_parsers.drain(10..);
        }

        parsers = new_parsers;
        prev_span = Some(token.span);
    }

    // there will always be a parser left,
    // since we always allow a token to be skipped
    let mut parser = parsers.into_iter().min_by_key(|p| p.error_cost).unwrap();
    parser.finish();

    let node = if parser.can_recover {
        Some(parser.stack_top.value.clone())
    } else {
        None
    };
    (node, parser.errors)
}

fn new_token_for_injection(kind: &Kind) -> Terminal {
    Terminal {
        kind: kind.clone(),
        text: kind.text().unwrap_or_default().to_string(),
        value: match kind {
            Kind::Keyword(Keyword(kw)) => Some(Value::String(kw.to_string())),
            Kind::Ident => Some(Value::String("my_name".to_string())),
            _ => None,
        },
        span: Span::default(),
    }
}

pub struct Spec {
    pub actions: Vec<IndexMap<Kind, Action>>,
    pub goto: Vec<IndexMap<String, usize>>,
    pub start: String,
    pub inlines: IndexMap<usize, u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Action {
    Shift(usize),
    Reduce(Reduce),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reduce {
    /// Index of the production in the associated production array
    pub production_id: usize,

    pub non_term: String,

    /// Number of arguments
    pub cnt: usize,
}

#[derive(Clone)]
pub enum CSTNode {
    Empty,
    Terminal(Terminal),
    Production(Production),
}

#[derive(Clone, Debug)]
pub struct Terminal {
    pub kind: Kind,
    pub text: String,
    pub value: Option<Value>,
    pub span: Span,
}

#[derive(Clone)]
pub struct Production {
    pub id: usize,
    pub args: Vec<CSTNode>,
}

struct StackNode<'p> {
    parent: Option<&'p StackNode<'p>>,

    state: usize,
    value: CSTNode,
}

struct Context<'s> {
    spec: &'s Spec,
    arena: &'s bumpalo::Bump,
}

#[derive(Clone)]
struct Parser<'s> {
    stack_top: &'s StackNode<'s>,

    /// sum of cost of every error recovery action
    error_cost: u16,

    /// number of nodes pushed to stack since last error
    node_count: u16,

    can_recover: bool,

    errors: Vec<Error>,
}

impl<'s> Context<'s> {
    fn new(spec: &'s Spec, arena: &'s bumpalo::Bump) -> Self {
        Context { spec, arena }
    }
}

impl<'s> Parser<'s> {
    fn act(&mut self, ctx: &'s Context, token: &Terminal) -> Result<(), ()> {
        // self.print_stack();
        // println!("INPUT: {}", token.text);

        loop {
            // find next action
            let Some(action) = ctx.spec.actions[self.stack_top.state].get(&token.kind) else {
                return Err(());
            };

            match action {
                Action::Shift(next) => {
                    // println!("   --> [shift {next}]");

                    // push on stack
                    self.stack_top = ctx.arena.alloc(StackNode {
                        parent: Some(self.stack_top),
                        state: *next,
                        value: CSTNode::Terminal(token.clone()),
                    });
                    return Ok(());
                }
                Action::Reduce(reduce) => {
                    self.reduce(ctx, reduce);
                }
            }
        }
    }

    fn reduce(&mut self, ctx: &'s Context, reduce: &'s Reduce) {
        let mut args = Vec::new();
        for _ in 0..reduce.cnt {
            args.push(self.stack_top.value.clone());
            self.stack_top = self.stack_top.parent.unwrap();
        }
        args.reverse();

        let value = CSTNode::Production(Production {
            id: reduce.production_id,
            args,
        });

        let nstate = self.stack_top.state;

        let next = *ctx.spec.goto[nstate].get(&reduce.non_term).unwrap();

        // inline (if there is an inlining rule)
        let mut value = value;
        if let CSTNode::Production(production) = value {
            if let Some(inline_position) = ctx.spec.inlines.get(&production.id) {
                // inline rule found
                let mut args = production.args;
                let span = get_span_of_nodes(&args);

                value = args.swap_remove(*inline_position as usize);

                extend_span(&mut value, span);
            } else {
                // place back
                value = CSTNode::Production(production);
            }
        }

        // push on stack
        self.stack_top = ctx.arena.alloc(StackNode {
            parent: Some(self.stack_top),
            state: next,
            value,
        });

        // println!(
        //     "   --> [reduce {} ::= ({} popped) at {}/{}]",
        //     production, cnt, state, nstate
        // );
        // self.print_stack();
    }

    pub fn finish(&mut self) {
        debug_assert!(matches!(
            &self.stack_top.value,
            CSTNode::Terminal(Terminal {
                kind: Kind::EOI,
                ..
            })
        ));
        self.stack_top = self.stack_top.parent.unwrap();

        // self.print_stack();
        // println!("   --> accept");

        #[cfg(debug_assertions)]
        {
            let first = self.stack_top.parent.unwrap();
            assert!(matches!(
                &first.value,
                CSTNode::Terminal(Terminal {
                    kind: Kind::Epsilon,
                    ..
                })
            ));
        }
    }

    #[cfg(never)]
    fn print_stack(&self) {
        let prefix = "STACK: ";

        let mut stack = Vec::new();
        let mut node = Some(self.stack_top);
        while let Some(n) = node {
            stack.push(n);
            node = n.parent.clone();
        }
        stack.reverse();

        let names = stack
            .iter()
            .map(|s| format!("{:?}", s.value))
            .collect::<Vec<_>>();

        let mut states = format!("{:6}", ' ');
        for (index, node) in stack.iter().enumerate() {
            let name_width = names[index].chars().count();
            states += &format!(" {:<width$}", node.state, width = name_width);
        }

        println!("{}{}", prefix, names.join(" "));
        println!("{}", states);
    }

    fn push_error(&mut self, error: Error, cost: u16) {
        self.errors.push(error);
        self.error_cost += cost;
        self.node_count = 0;
    }

    fn node_successful(&mut self) {
        self.node_count += 1;
    }

    /// Error cost, subtracted by a function of successfully parsed nodes.
    fn adjusted_cost(&self) -> u16 {
        let x = self.node_count.saturating_sub(3);
        self.error_cost.saturating_sub(x * x)
    }

    fn has_recovered(&self) -> bool {
        self.can_recover && self.adjusted_cost() == 0
    }
}

fn get_span_of_nodes(args: &[CSTNode]) -> Option<Span> {
    let start = args.iter().find_map(|x| match x {
        CSTNode::Terminal(t) => Some(t.span.start),
        _ => None,
    })?;
    let end = args.iter().rev().find_map(|x| match x {
        CSTNode::Terminal(t) => Some(t.span.end),
        _ => None,
    })?;
    Some(Span { start, end })
}

fn extend_span(value: &mut CSTNode, span: Option<Span>) {
    let Some(span) = span else {
        return;
    };

    let CSTNode::Terminal(terminal) = value else {
        return
    };

    if span.start < terminal.span.start {
        terminal.span.start = span.start;
    }
    if span.end > terminal.span.end {
        terminal.span.end = span.end;
    }
}

const ERROR_COST_INJECT_MAX: u16 = 15;
const ERROR_COST_SKIP: u16 = 3;

fn error_cost(kind: &Kind) -> u16 {
    use Kind::*;

    match kind {
        Ident => 9,
        Substitution => 8,
        Keyword(_) => 10,

        Dot => 5,
        OpenBrace | OpenBracket | OpenParen => 5,

        CloseBrace | CloseBracket | CloseParen => 1,

        Namespace => 10,
        Semicolon | Comma | Colon => 2,
        Eq => 5,

        At => 6,
        IntConst => 8,

        Assign | Arrow => 5,

        _ => 100, // forbidden
    }
}

impl std::fmt::Display for Terminal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.text.is_empty() {
            return write!(f, "{}", self.kind.user_friendly_text().unwrap_or_default());
        }

        match self.kind {
            Kind::Ident => write!(f, "'{}'", &quote_name(&self.text)),
            Kind::Keyword(Keyword(kw)) => write!(f, "keyword '{}'", kw.to_ascii_uppercase()),
            _ => write!(f, "'{}'", self.text),
        }
    }
}

impl Default for CSTNode {
    fn default() -> Self {
        CSTNode::Empty
    }
}
