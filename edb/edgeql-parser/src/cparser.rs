use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::helpers::quote_string;

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

pub fn load(jspec: &str) -> Result<Spec, String> {
    let v = serde_json::from_str::<SpecJson>(jspec).map_err(|e| format!("Error: {e}"))?;

    Ok(Spec {
        actions: v.actions.into_iter().map(HashMap::from_iter).collect(),
        goto: v.goto.into_iter().map(HashMap::from_iter).collect(),
        start: v.start,
    })
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum CSTNode<'a> {
    Empty,
    Token {
        kind: &'a str,
        text: String,
        value: Option<String>,
    },
    Value {
        nonterm: String,
        production: String,
        args: Vec<CSTNode<'a>>,
    },
}

#[derive(Debug)]
struct StackNode<'a> {
    state: usize,
    value: CSTNode<'a>,
}

pub struct Parser<'s, 't> {
    spec: &'s Spec,
    stack: Vec<StackNode<'t>>,
}

impl<'s, 't> Parser<'s, 't> {
    fn new(spec: &'s Spec) -> Self {
        Parser {
            spec,
            stack: vec![StackNode {
                state: 0,
                value: CSTNode::Empty,
            }],
        }
    }

    pub fn act<'a>(
        &mut self,
        kind: &'t str,
        text: String,
        value: Option<String>,
    ) -> Result<(), String> {
        self.print_stack();
        println!("INPUT: {text}");

        loop {
            let state = self.stack.last().unwrap().state;

            let Some(action) = self.spec.actions[state].get(kind) else {
                return Err(format!("Unexpected: {}", quote_string(&text)));
            };

            match action {
                Action::Shift(next) => {
                    println!("   --> [shift {next}]");

                    self.stack.push(StackNode {
                        state: *next,
                        value: CSTNode::Token { kind, text, value },
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

                    let value = CSTNode::Value {
                        nonterm: nonterm.clone(),
                        production: production.clone(),
                        args,
                    };

                    let nstate = self.stack.last().unwrap().state;

                    println!(
                        "   --> [reduce {} ::= ({} popped) at {}/{}]",
                        production, cnt, state, nstate
                    );

                    let next = *self.spec.goto[nstate]
                        .get(nonterm)
                        .ok_or(format!("{} at {} fucked", nonterm, nstate))?;

                    self.stack.push(StackNode { state: next, value });

                    self.print_stack();
                }
            }
        }
        Ok(())
    }

    pub fn eoi(&mut self) -> Result<(), String> {
        const EOI: &str = "<$>";

        self.act(EOI, "".to_string(), None)?;

        let eof = self.stack.pop();
        debug_assert!(eof.is_some());
        debug_assert!(matches!(
            eof.unwrap().value,
            CSTNode::Token { kind: EOI, .. }
        ));

        self.print_stack();
        println!("   --> accept");

        #[cfg(debug_assertions)]
        {
            let first = self.stack.first().unwrap();
            assert!(matches!(first.value, CSTNode::Token { kind: "<e>", .. }));
        }
        Ok(())
    }

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

pub fn cparse(
    jspec: String,
    input: Vec<(&str, String, Option<String>)>,
) -> Result<CSTNode, String> {
    let spec: Spec = load(&jspec)?;
    let mut parser = Parser::new(&spec);

    for (tok, text, value) in input {
        parser.act(tok, text, value)?;
    }
    parser.eoi()?;

    let out = parser.stack.pop().ok_or("stack empty")?;
    Ok(out.value)
}

impl<'t> std::fmt::Debug for CSTNode<'t> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("<e>"),
            Self::Token { text, .. } => f.write_str(text),
            Self::Value { production, .. } => write!(f, "{production}"),
        }
    }
}
