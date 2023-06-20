use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::position::Span;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Action {
    Shift(usize),
    Reduce {
        nonterm: String,
        production: String,
        cnt: usize,
    },
}

pub struct Spec {
    pub actions: Vec<HashMap<String, Action>>,
    pub goto: Vec<HashMap<String, usize>>,
    pub start: String,
}

#[derive(Deserialize)]
pub struct SpecJson {
    pub actions: Vec<Vec<(String, Action)>>,
    pub goto: Vec<Vec<(String, usize)>>,
    pub start: String,
}

impl Spec {
    pub fn from_json(j_spec: &str) -> Result<Spec, String> {
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
    Token(ParserToken),
    Production {
        non_term: String,
        production: String,
        args: Vec<CSTNode>,
    },
}

#[derive(Serialize, Default)]
pub struct ParserToken {
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

pub struct Parser<'s> {
    spec: &'s Spec,
    stack: Vec<StackNode>,
}

impl<'s> Parser<'s> {
    fn new(spec: &'s Spec) -> Self {
        Parser {
            spec,
            stack: vec![StackNode {
                state: 0,
                value: CSTNode::Empty,
            }],
        }
    }

    pub fn act(&mut self, token: ParserToken) -> Result<(), String> {
        // self.print_stack();
        // println!("INPUT: {}", token.text);

        loop {
            let state = self.stack.last().unwrap().state;

            let Some(action) = self.spec.actions[state].get(&token.kind) else {
                return Err(format!("Unexpected {token}"));
            };

            match action {
                Action::Shift(next) => {
                    // println!("   --> [shift {next}]");

                    self.stack.push(StackNode {
                        state: *next,
                        value: CSTNode::Token(token),
                    });
                    break;
                }
                Action::Reduce {
                    nonterm,
                    production,
                    cnt,
                } => {
                    let args = self
                        .stack
                        .drain((self.stack.len() - cnt)..)
                        .map(|n| n.value)
                        .collect();

                    let value = CSTNode::Production {
                        non_term: nonterm.clone(),
                        production: production.clone(),
                        args,
                    };

                    let nstate = self.stack.last().unwrap().state;

                    // println!(
                    //     "   --> [reduce {} ::= ({} popped) at {}/{}]",
                    //     production, cnt, state, nstate
                    // );

                    let next = *self.spec.goto[nstate]
                        .get(nonterm)
                        .ok_or(format!("{} at {} fucked", nonterm, nstate))?;

                    self.stack.push(StackNode { state: next, value });

                    // self.print_stack();
                }
            }
        }
        Ok(())
    }

    pub fn eoi(&mut self) -> Result<(), String> {
        const EOI: &str = "<$>";

        self.act(ParserToken {
            kind: EOI.to_string(),
            ..Default::default()
        })?;

        let eof = self.stack.pop();
        debug_assert!(eof.is_some());
        debug_assert!(matches!(
            eof.unwrap().value,
            CSTNode::Token(ParserToken { kind, .. }) if kind == EOI
        ));

        // self.print_stack();
        // println!("   --> accept");

        #[cfg(debug_assertions)]
        {
            let first = self.stack.first().unwrap();
            assert!(matches!(
                &first.value,
                CSTNode::Token(ParserToken { kind, .. }) if kind == "<e>"
            ));
        }
        Ok(())
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
}

pub fn parse<'s, 't>(spec: &'s Spec, input: Vec<ParserToken>) -> Result<CSTNode, String> {
    let mut parser = Parser::new(&spec);

    for token in input {
        parser.act(token)?;
    }
    parser.eoi()?;

    let out = parser.stack.pop().ok_or("stack empty")?;
    Ok(out.value)
}

impl std::fmt::Debug for CSTNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("<e>"),
            Self::Token(t) => f.write_str(&t.text),
            Self::Production { production, .. } => write!(f, "{production}"),
        }
    }
}

impl std::fmt::Display for ParserToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        match self.kind.as_str() {
            "EOF" => {
                f.write_str("end of line")
            }
            _ => write!(f, "token: {}", self.kind),
        }
    }
}
