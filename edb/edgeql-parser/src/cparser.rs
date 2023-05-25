use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

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
        token: &'a String,
        text: &'a String,
    },
    Value {
        nonterm: String,
        production: String,
        args: Vec<CSTNode<'a>>,
    },
}

struct StackNode<'a> {
    state: usize,
    value: CSTNode<'a>,
}

fn cparse_main<'a>(spec: &Spec, input: &'a [(String, String)]) -> Result<CSTNode<'a>, String> {
    let mut stack: Vec<StackNode> = Vec::new();
    stack.push(StackNode {
        state: 0,
        value: CSTNode::Empty,
    });

    let actions = &spec.actions;

    for (tok, text) in input {
        loop {
            let state = stack[stack.len() - 1].state;
            // println!("seeing {} at {}", tok, state);

            let action = match actions[state].get(tok) {
                Some(v) => v,
                None => Err(format!("unexpected token {} in state {}", tok, state))?,
            };
            match action {
                Action::Shift(next) => {
                    // println!("shifting {}/{} at {}", tok, next, state);
                    stack.push(StackNode {
                        state: *next,
                        value: CSTNode::Token { token: tok, text },
                    });
                    break;
                }
                Action::Reduce {
                    nonterm,
                    production,
                    cnt,
                } => {
                    // let next = spec.goto[state][nonterm];

                    let args = stack
                        .drain((stack.len() - cnt)..)
                        .map(|n| n.value)
                        .collect();

                    let value = CSTNode::Value {
                        nonterm: nonterm.clone(),
                        production: production.clone(),
                        args,
                    };

                    let nstate = stack[stack.len() - 1].state;
                    // println!("reducing {}/{} ({} popped) at {}/{}",
                    //          nonterm, prod, cnt, state, nstate);
                    let next = *spec.goto[nstate]
                        .get(nonterm)
                        .ok_or(format!("{} at {} fucked", nonterm, nstate))?;

                    stack.push(StackNode { state: next, value });
                }
            }
        }
    }

    stack.pop();
    let out = stack.pop().ok_or("stack empty")?;
    Ok(out.value)
}

pub fn cparse(jspec: String, input: &[(String, String)]) -> Result<CSTNode, String> {
    let spec = load(&jspec)?;
    cparse_main(&spec, input)
}
