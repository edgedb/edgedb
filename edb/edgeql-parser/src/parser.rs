use indexmap::IndexMap;

use crate::helpers::quote_name;
use crate::keywords::Keyword;
use crate::position::Span;
use crate::tokenizer::{Token, Value, Error, Kind};

pub struct Context<'s> {
    spec: &'s Spec,
    arena: bumpalo::Bump,
}

impl<'s> Context<'s> {
    pub fn new(spec: &'s Spec) -> Self {
        let arena = bumpalo::Bump::new();
        Context { spec, arena }
    }
}

pub fn parse<'a>(input: &'a [Terminal], ctx: &'a Context) -> (Option<&'a CSTNode<'a>>, Vec<Error>) {
    let stack_top = ctx.arena.alloc(StackNode {
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

    // append EIO
    let end = input.last().map(|t| t.span.end).unwrap_or_default();
    let eio: &Terminal = ctx.arena.alloc(Terminal {
        kind: Kind::EOI,
        span: Span { start: end, end },
        text: "".to_string(),
        value: None,
    });
    let input = input.iter().chain(Some(eio));

    let mut parsers = vec![initial_track];
    let mut prev_span: Option<Span> = None;

    for token in input {
        let mut new_parsers = Vec::with_capacity(parsers.len() + 5);

        while let Some(mut parser) = parsers.pop() {
            let res = parser.act(ctx, token);

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

                    let injection = new_token_for_injection(token_kind, &ctx.arena);

                    let cost = error_cost(token_kind);
                    let error = Error::new(format!("Missing {injection}")).with_span(gap_span);
                    inject.push_error(error, cost);

                    if inject.error_cost <= ERROR_COST_INJECT_MAX {
                        // println!("   --> [inject {injection}]");

                        if inject.act(ctx, injection).is_ok() {
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
        Some(&parser.stack_top.value)
    } else {
        None
    };
    (node, parser.errors)
}

fn new_token_for_injection<'a>(kind: &Kind, arena: &'a bumpalo::Bump) -> &'a Terminal {
    arena.alloc(Terminal {
        kind: kind.clone(),
        text: kind.text().unwrap_or_default().to_string(),
        value: match kind {
            Kind::Keyword(Keyword(kw)) => Some(Value::String(kw.to_ascii_lowercase())),
            Kind::Ident => Some(Value::String("my_name".to_string())),
            _ => None,
        },
        span: Span::default(),
    })
}

pub struct Spec {
    pub actions: Vec<IndexMap<Kind, Action>>,
    pub goto: Vec<IndexMap<String, usize>>,
    pub start: String,
    pub inlines: IndexMap<usize, u8>,
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum Action {
    Shift(usize),
    Reduce(Reduce),
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Reduce {
    /// Index of the production in the associated production array
    pub production_id: usize,

    pub non_term: String,

    /// Number of arguments
    pub cnt: usize,
}

#[derive(Clone)]
pub enum CSTNode<'a> {
    Empty,
    Terminal(&'a Terminal),
    Production(Production<'a>),
}
#[derive(Clone, Debug)]
pub struct Terminal {
    pub kind: Kind,
    pub text: String,
    pub value: Option<Value>,
    pub span: Span,
}

#[derive(Clone)]
pub struct Production<'a> {
    pub id: usize,
    pub args: bumpalo::collections::Vec<'a, CSTNode<'a>>,
}

struct StackNode<'p> {
    parent: Option<&'p StackNode<'p>>,

    state: usize,
    value: CSTNode<'p>,
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

impl<'s> Parser<'s> {
    fn act(&mut self, ctx: &'s Context, token: &'s Terminal) -> Result<(), ()> {
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
                    self.push_on_stack(ctx, *next, CSTNode::Terminal(token));
                    return Ok(());
                }
                Action::Reduce(reduce) => {
                    self.reduce(ctx, reduce);
                }
            }
        }
    }

    fn reduce(&mut self, ctx: &'s Context, reduce: &'s Reduce) {
        let mut args = bumpalo::collections::Vec::with_capacity_in(reduce.cnt, &ctx.arena);
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

                extend_span(&mut value, span, ctx);
            } else {
                // place back
                value = CSTNode::Production(production);
            }
        }

        self.push_on_stack(ctx, next, value);

        // println!(
        //     "   --> [reduce {} ::= ({} popped) at {}/{}]",
        //     production, cnt, state, nstate
        // );
        // self.print_stack();
    }

    pub fn push_on_stack(&mut self, ctx: &'s Context, state: usize, value: CSTNode<'s>) {
        let node = StackNode {
            parent: Some(self.stack_top),
            state,
            value,
        };
        self.stack_top = ctx.arena.alloc(node);
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

fn extend_span<'a>(value: &mut CSTNode<'a>, span: Option<Span>, ctx: &'a Context) {
    let Some(span) = span else {
        return;
    };

    let CSTNode::Terminal(terminal) = value else {
        return
    };

    let new_term = ctx.arena.alloc(terminal.clone());

    if span.start < new_term.span.start {
        new_term.span.start = span.start;
    }
    if span.end > new_term.span.end {
        new_term.span.end = span.end;
    }
    *terminal = new_term;
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
            Kind::Keyword(Keyword(kw)) => write!(f, "keyword '{kw}'"),
            _ => write!(f, "'{}'", self.text),
        }
    }
}

impl<'a> Default for CSTNode<'a> {
    fn default() -> Self {
        CSTNode::Empty
    }
}

impl Terminal {
    pub fn from_token(token: Token) -> Self {
        Terminal {
            kind: token.kind,
            text: token.text.into(),
            value: token.value,
            span: token.span,
        }
    }
}

#[cfg(feature = "serde")]
impl Spec {
    pub fn from_json(j_spec: &str) -> Result<Spec, String> {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct SpecJson {
            pub actions: Vec<Vec<(String, Action)>>,
            pub goto: Vec<Vec<(String, usize)>>,
            pub start: String,
            pub inlines: Vec<(usize, u8)>,
        }

        let v = serde_json::from_str::<SpecJson>(j_spec).map_err(|e| e.to_string())?;

        let actions = v
            .actions
            .into_iter()
            .map(|x| x.into_iter().map(|(k, a)| (get_token_kind(&k), a)))
            .map(IndexMap::from_iter)
            .collect();
        let goto = v.goto.into_iter().map(IndexMap::from_iter).collect();
        let inlines = IndexMap::from_iter(v.inlines);
        Ok(Spec {
            actions,
            goto,
            start: v.start,
            inlines,
        })
    }
}

#[cfg(feature = "serde")]
fn get_token_kind(token_name: &str) -> Kind {
    use Kind::*;

    match token_name {
        "+" => Add,
        "&" => Ampersand,
        "@" => At,
        ".<" => BackwardLink,
        "}" => CloseBrace,
        "]" => CloseBracket,
        ")" => CloseParen,
        "??" => Coalesce,
        ":" => Colon,
        "," => Comma,
        "++" => Concat,
        "/" => Div,
        "." => Dot,
        "**" => DoubleSplat,
        "=" => Eq,
        "//" => FloorDiv,
        "%" => Modulo,
        "*" => Mul,
        "::" => Namespace,
        "{" => OpenBrace,
        "[" => OpenBracket,
        "(" => OpenParen,
        "|" => Pipe,
        "^" => Pow,
        ";" => Semicolon,
        "-" => Sub,

        "?!=" => DistinctFrom,
        ">=" => GreaterEq,
        "<=" => LessEq,
        "?=" => NotDistinctFrom,
        "!=" => NotEq,
        "<" => Less,
        ">" => Greater,

        "IDENT" => Ident,
        "EOF" => EOF,
        "<$>" => EOI,
        "<e>" => Epsilon,

        "BCONST" => BinStr,
        "FCONST" => FloatConst,
        "ICONST" => IntConst,
        "NFCONST" => DecimalConst,
        "NICONST" => BigIntConst,
        "SCONST" => Str,

        "+=" => AddAssign,
        "->" => Arrow,
        ":=" => Assign,
        "-=" => SubAssign,

        "ARGUMENT" => Argument,
        "SUBSTITUTION" => Substitution,

        _ => {
            let mut token_name = token_name.to_uppercase();

            if let Some(rem) = token_name.strip_prefix("DUNDER") {
                token_name = format!("__{rem}__");
            }

            let kw = crate::keywords::lookup_all(&token_name)
                .unwrap_or_else(|| panic!("unknown keyword {token_name}"));
            Keyword(kw)
        }
    }
}
