use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::helpers::quote_name;
use crate::position::Span;
use crate::tokenizer::Error;

pub fn parse<'s, 't>(spec: &'s Spec, input: Vec<Terminal>) -> (Option<CSTNode>, Vec<Error>) {
    let mut parser = Parser::new(&spec);

    for token in input {
        {
            let ref mut this = parser;
            this.act(token);
        };
    }
    parser.eoi();

    let node = parser.stack.pop().map(|n| n.value);
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

#[derive(Serialize)]
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

#[derive(Serialize, Default)]
pub struct Terminal {
    pub kind: String,
    pub text: String,
    pub value: Option<String>,
    pub span: Span,
}

#[derive(Debug)]
struct StackNode {
    state: usize,
    value: CSTNode,
}

struct RecoveryPath {
    actions: Vec<RecoveryAction>,
    cost: u16,
}

enum RecoveryAction {
    Pop,
    SkipInto(),
}

pub struct Parser<'s> {
    spec: &'s Spec,
    stack: Vec<StackNode>,
    errors: Vec<Error>,
}

impl<'s> Parser<'s> {
    fn new(spec: &'s Spec) -> Self {
        Parser {
            spec,
            stack: vec![StackNode {
                state: 0,
                value: CSTNode::Empty,
            }],
            errors: Vec::new(),
        }
    }

    fn act(&mut self, token: Terminal) {
        // self.print_stack();
        // println!("INPUT: {}", token.text);

        loop {
            // find next action
            let action = loop {
                // base case
                if let Some(action) = self
                    .stack
                    .last()
                    .and_then(|s| self.spec.actions[s.state].get(&(&token).kind))
                {
                    break action;
                }

                let error = Error::new(format!("Unexpected {token}")).with_span(token.span);
                self.errors.push(error);

                // special case: try to recover
                if let Some(recovery) = self.find_recovery_path(&token) {
                    self.apply_recovery(recovery);

                    // retry lookup
                    if let Some(action) = self
                        .stack
                        .last()
                        .and_then(|s| self.spec.actions[s.state].get(&(&token).kind))
                    {
                        break action;
                    }
                }

                // fail
                return;
            };

            match action {
                Action::Shift(next) => {
                    // println!("   --> [shift {next}]");

                    self.stack.push(StackNode {
                        state: *next,
                        value: CSTNode::Terminal(token),
                    });
                    break;
                }
                Action::Reduce(reduce) => {
                    let res = self.reduce(reduce);

                    if let Err(message) = res {
                        self.errors.push(Error::new(message).with_span(token.span));
                        return;
                    }
                }
            }
        }
    }

    fn reduce(&mut self, reduce: &Reduce) -> Result<(), String> {
        let args = self
            .stack
            .drain((self.stack.len() - reduce.cnt)..)
            .map(|n| n.value)
            .collect();

        let value = CSTNode::Production {
            non_term: reduce.non_term.clone(),
            production: reduce.production.clone(),
            args,
        };

        let nstate = self.stack.last().unwrap().state;

        let next = *self.spec.goto[nstate]
            .get(&reduce.non_term)
            .ok_or_else(|| format!("{} at {} fucked", reduce.non_term, nstate))?;

        self.stack.push(StackNode { state: next, value });

        // println!(
        //     "   --> [reduce {} ::= ({} popped) at {}/{}]",
        //     production, cnt, state, nstate
        // );
        // self.print_stack();
        Ok(())
    }

    pub fn eoi(&mut self) {
        const EOI: &str = "<$>";

        self.act(Terminal {
            kind: EOI.to_string(),
            ..Default::default()
        });

        let eof = self.stack.pop();
        debug_assert!(eof.is_some());
        debug_assert!(matches!(
            eof.unwrap().value,
            CSTNode::Terminal(Terminal { kind, .. }) if kind == EOI
        ));

        // self.print_stack();
        // println!("   --> accept");

        #[cfg(debug_assertions)]
        {
            let first = self.stack.first().unwrap();
            assert!(matches!(
                &first.value,
                CSTNode::Terminal(Terminal { kind, .. }) if kind == "<e>"
            ));
        }
    }

    #[allow(dead_code)]
    fn print_stack(&self) {
        let prefix = "STACK: ";

        let names = self
            .stack
            .iter()
            .map(|s| format!("{:?}", s.value))
            .collect::<Vec<_>>();

        let mut states = format!("{:6}", ' ');
        for (index, node) in self.stack.iter().enumerate() {
            let name_width = names[index].chars().count();
            states += &format!(" {:<width$}", node.state, width = name_width);
        }

        println!("{}{}", prefix, names.join(" "));
        println!("{}", states);
    }

    fn find_recovery_path(&self, _token: &Terminal) -> Option<RecoveryPath> {
        let mut path = RecoveryPath::new();
        path.add(RecoveryAction::Pop);

        Some(path)
    }

    fn apply_recovery(&mut self, recovery: RecoveryPath) {
        for action in recovery.actions {
            match action {
                RecoveryAction::Pop => {
                    self.stack.pop();
                }
                RecoveryAction::SkipInto() => todo!(),
            }
        }
    }
}

impl RecoveryPath {
    fn new() -> Self {
        RecoveryPath {
            actions: Vec::new(),
            cost: 0,
        }
    }

    fn add(&mut self, action: RecoveryAction) {
        self.cost += action.cost();
        self.actions.push(action);
    }
}

impl RecoveryAction {
    fn cost(&self) -> u16 {
        match self {
            RecoveryAction::Pop => 1,
            RecoveryAction::SkipInto() => 5,
        }
    }
}

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
