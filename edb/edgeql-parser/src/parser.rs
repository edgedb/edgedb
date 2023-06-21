use std::collections::HashMap;

use phf::phf_map;
use serde::Deserialize;
use serde::Serialize;

use crate::helpers::quote_name;
use crate::position::Span;
use crate::tokenizer::Error;

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
        errors: Vec::new(),
    };

    let ctx = Context::new(spec, &arena);

    // append EIO
    let end = input.last().map(|t| t.span.end).unwrap_or_default();
    let eio = Terminal {
        kind: "<$>".to_string(),
        span: Span { start: end, end },
        ..Default::default()
    };
    let input = [input, vec![eio]].concat();

    let mut parsers = vec![initial_track];

    for token in input {
        let mut new_parsers = Vec::with_capacity(parsers.len() + 5);

        while let Some(mut parser) = parsers.pop() {
            let res = parser.act(&ctx, &token);

            if res.is_ok() {
                // base case: ok
                new_parsers.push(parser);
            } else {
                // error: try to recover

                // option 1: inject a token
                let possible_actions = &ctx.spec.actions[parser.stack_top.state];
                for token_kind in possible_actions.keys() {
                    let mut inject = parser.clone();

                    let injection = Terminal {
                        kind: token_kind.clone(),
                        ..Default::default()
                    };

                    let cost = ERROR_COST.get(token_kind).cloned().unwrap_or(KEYWORD);
                    let error = Error::new(format!("Missing {injection}")).with_span(token.span);
                    if inject.try_push_error(error, cost) {
                        // println!("   --> [inject {injection}]");

                        if inject.act(&ctx, &injection).is_ok() {
                            // insert into parsers, to retry the original token
                            parsers.push(inject);
                        }
                    }
                }

                // option 2: skip the token
                if token.kind != "EOF" {
                    let mut skip = parser;
                    let error = Error::new(format!("Unexpected {token}")).with_span(token.span);
                    if skip.try_push_error(error, ERROR_COST_SKIP) {
                        // println!("   --> [skip]");

                        // insert into new_parsers, so the token is skipped
                        new_parsers.push(skip);
                    }
                }
            }
        }

        parsers = new_parsers;
    }

    // TODO: handle error here
    let mut parser = parsers.into_iter().min_by_key(|p| p.error_cost).unwrap();
    parser.finish();

    let node = Some(parser.stack_top.value.clone());
    (node, parser.errors)
}

pub struct Spec {
    pub actions: Vec<HashMap<String, Action>>,
    pub goto: Vec<HashMap<String, usize>>,
    pub start: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Action {
    Shift(usize),
    Reduce(Reduce),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reduce {
    pub non_term: String,
    pub production: String,
    pub cnt: usize,
}

impl Spec {
    pub fn from_json(j_spec: &str) -> Result<Spec, String> {
        #[derive(Debug, Deserialize)]
        pub struct SpecJson {
            pub actions: Vec<Vec<(String, Action)>>,
            pub goto: Vec<Vec<(String, usize)>>,
            pub start: String,
        }

        let v = serde_json::from_str::<SpecJson>(j_spec).map_err(|e| e.to_string())?;

        Ok(Spec {
            actions: v.actions.into_iter().map(HashMap::from_iter).collect(),
            goto: v.goto.into_iter().map(HashMap::from_iter).collect(),
            start: v.start,
        })
    }
}

#[derive(Serialize, Clone)]
#[serde(untagged)]
pub enum CSTNode {
    Empty,
    Terminal(Terminal),
    Production {
        non_term: String,
        production: String,
        args: Vec<CSTNode>,
    },
}

#[derive(Serialize, Default, Clone)]
pub struct Terminal {
    pub kind: String,
    pub text: String,
    pub value: Option<String>,
    pub span: Span,
}

#[derive(Debug)]
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

    error_cost: u16,
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

        let value = CSTNode::Production {
            non_term: reduce.non_term.clone(),
            production: reduce.production.clone(),
            args,
        };

        let nstate = self.stack_top.state;

        let next = *ctx.spec.goto[nstate]
            .get(&reduce.non_term)
            .unwrap_or_else(|| panic!("{nstate} {} {}", reduce.non_term, reduce.production));

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
            CSTNode::Terminal(Terminal { kind, .. }) if kind == "<$>"
        ));
        self.stack_top = self.stack_top.parent.unwrap();

        // self.print_stack();
        // println!("   --> accept");

        #[cfg(debug_assertions)]
        {
            let first = self.stack_top.parent.unwrap();
            assert!(matches!(
                &first.value,
                CSTNode::Terminal(Terminal { kind, .. }) if kind == "<e>"
            ));
        }
    }

    #[allow(dead_code)]
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

    fn try_push_error(&mut self, error: Error, cost: u16) -> bool {
        self.errors.push(error);
        self.error_cost += cost;
        return self.error_cost <= ERROR_COST_MAX;
    }
}

const ERROR_COST_MAX: u16 = 15;
const ERROR_COST_SKIP: u16 = 2;

const FORBIDDEN: u16 = 100;
const KEYWORD: u16 = 10;

static ERROR_COST: phf::Map<&str, u16> = phf_map! {
    "IDENT" => 10,
    "ARGUMENT" => FORBIDDEN,
    "EOF" => FORBIDDEN,
    "SUBSTITUTION" => 8,
    "NAMEDONLY" => 10,
    "SETANNOTATION" => 10,
    "SETTYPE" => 10,
    "EXTENSIONPACKAGE" => 10,
    "ORDERBY" => 10,

    "." => 5,
    ".<" => 5,
    "[" => 5,
    "(" => 5,
    "{" => 5,

    "]" => 1,
    ")" => 1,
    "}" => 1,

    "::" => 10,
    "**" => FORBIDDEN,
    "??" => FORBIDDEN,
    ":" => 5,
    ";" => 5,
    "," => 5,
    "+" => FORBIDDEN,
    "++" => FORBIDDEN,
    "-" => FORBIDDEN,
    "*" => FORBIDDEN,
    "/" => FORBIDDEN,
    "//" => FORBIDDEN,
    "%" => FORBIDDEN,
    "^" => FORBIDDEN,
    "<" => FORBIDDEN,
    ">" => FORBIDDEN,
    "=" => 5,
    "&" => FORBIDDEN,
    "|" => FORBIDDEN,
    "@" => 5,
    "ICONST" => 8,
    "NICONST" => FORBIDDEN,
    "FCONST" => FORBIDDEN,
    "NFCONST" => FORBIDDEN,
    "BCONST" => FORBIDDEN,
    "SCONST" => FORBIDDEN,
    "OP" => FORBIDDEN,
    ">=" => FORBIDDEN,
    "<=" => FORBIDDEN,
    "!=" => FORBIDDEN,
    "?!=" => FORBIDDEN,
    "?=" => FORBIDDEN,
    "ASSIGN" => 10,
    "ADDASSIGN" => 10,
    "REMASSIGN" => 10,
    "ARROW" => 10,
    "<e>" => FORBIDDEN, // epsilon
    "<$>" => FORBIDDEN, // eoi
};

impl std::fmt::Debug for CSTNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("<e>"),
            Self::Terminal(t) => f.write_str(&t.text),
            Self::Production { production, .. } => write!(f, "{production}"),
        }
    }
}

impl std::fmt::Display for Terminal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind.as_str() {
            "EOF" => f.write_str("end of line"),
            "IDENT" => f.write_str(&quote_name(&self.text)),
            _ => write!(f, "token: {}", self.kind),
        }
    }
}

impl Default for CSTNode {
    fn default() -> Self {
        CSTNode::Empty
    }
}
