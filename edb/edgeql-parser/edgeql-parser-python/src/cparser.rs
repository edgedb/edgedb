use std::collections::HashMap;
// use std::iter::Peekable;
// use std::slice::Iter;
// use std::str::FromStr;
use serde_json::Value;
use serde_json::json;

#[derive(Debug)]
pub enum Action {
    Shift(usize),
    Reduce(String, String, usize),
}

pub struct Spec {
    pub actions: Vec<HashMap<String, Action>>,
    pub goto: Vec<HashMap<String, usize>>,
    pub start: String,
}

pub fn load(jspec: &str) -> Result<Spec, String>
{
    let v: Value = serde_json::from_str(jspec).map_err(
        |e| format!("Error: {e}"))?;

    let mut sactions = Vec::new();

    // ERRORS
    let actions = v.get("actions").ok_or("missing actions")?.as_array().
        ok_or("actions not array")?;
    let goto = v.get("goto").ok_or("missing goto")?.as_array().
        ok_or("goto not array")?;

    for jaction in actions {
        // println!("tok: {:?}", jaction);
        let sacts = jaction.as_array(). ok_or("actions not array")?;
        let mut acts = HashMap::new();

        for sact in sacts {
            let stok = sact.get(0).ok_or("bad entry")?.as_str().
                ok_or("tok not string")?;
            let sact2 = sact.get(1).ok_or("bad entry")?;

            let act = match sact2 {
                Value::Number(next) => {
                    Action::Shift(next.as_u64().ok_or("bad state")? as usize)
                }
                Value::Object(m) => {
                    let nonterm = m.get("nonterm").ok_or("no nonterm")?.
                        as_str().ok_or("nonterm not string")?;
                    let prod = m.get("production").ok_or("no prod")?.
                        as_str().ok_or("prod not string")?;
                    let cnt = m.get("cnt").ok_or("no cnt")?.
                        as_u64().ok_or("cnt not int")?;

                    Action::Reduce(
                        String::from(nonterm), String::from(prod), cnt as usize)
                }
                _ => {
                    Err("shit")?
                }
            };
            acts.insert(String::from(stok), act);

        }
        sactions.push(acts);
    }


    let mut sgoto = Vec::new();

    for jgoto in goto {
        // println!("tok: {:?}", jaction);
        let jsgotos = jgoto.as_array(). ok_or("goto not array")?;
        let mut gotos = HashMap::new();

        for jgoto in jsgotos {
            let stok = jgoto.get(0).ok_or("bad entry")?.as_str().
                ok_or("tok not string")?;
            let snext = jgoto.get(1).ok_or("bad entry")?.
                as_u64().ok_or("bad state")? as usize;
            gotos.insert(String::from(stok), snext);
        }
        sgoto.push(gotos);
    }


    let start = v.get("start").ok_or("no start")?.as_str().ok_or("bad start")?;

    // println!("SPEC: {:?}", spec);

    Ok(Spec { actions: sactions, goto: sgoto, start: String::from(start) })
}

fn cparse_main(
    spec: &Spec, input: &[(String, String)]
) -> Result<serde_json::Value, String> {
    // let mut stack: Vec<(&str, usize, serde_json::Value)> = Vec::new();
    // stack.push((&*spec.1, 0, Value::Null));
    let mut stack: Vec<(usize, serde_json::Value)> = Vec::new();
    stack.push((0, Value::Null));

    let actions = &spec.actions;

    for (tok, text) in input {
        loop {
            let state = stack[stack.len() - 1].0;
            // println!("seeing {} at {}", tok, state);

            let action = match actions[state].get(tok) {
                Some(v) => { v }
                None => {
                    Err(format!("unexpected token {} in state {}", tok, state))?
                }
            };
            match action {
                Action::Shift(next) => {
                    // println!("shifting {}/{} at {}", tok, next, state);
                    stack.push((
                        *next,
                        json!({"token": tok, "text": text}),
                    ));
                    break;
                }
                Action::Reduce(nonterm, prod, cnt) => {
                    // let next = spec.goto[state][nonterm];
                    let mut parts = Vec::new();
                    for _ in 0..*cnt {
                        parts.push(stack.pop().unwrap().1);
                    }
                    let rparts: Vec<_> = parts.into_iter().rev().collect();
                    let val = json!(
                        {"nonterm": nonterm, "prod": prod, "args": rparts}
                    );

                    let nstate = stack[stack.len() - 1].0;
                    // println!("reducing {}/{} ({} popped) at {}/{}",
                    //          nonterm, prod, cnt, state, nstate);
                    let next = *spec.goto[nstate].get(nonterm)
                        .ok_or(format!("{} at {} fucked", nonterm, nstate))?;

                    stack.push((next, val));
                }
            }

        }

    }

    stack.pop();
    let out = stack.pop().ok_or("stack empty")?;
    Ok(out.1)
}


pub fn cparse(
    jspec: String, input: Vec<(String, String)>) -> Result<String, String>
{
    let spec = load(&*jspec)?;
    let val = cparse_main(&spec, &*input)?;

    Ok(val.to_string())
}
