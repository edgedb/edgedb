use memchr::memmem::find;

#[derive(Debug, PartialEq)]
pub struct Continuation {
    position: usize,
    braces: Vec<u8>,
}

/// Returns index of semicolon, or position where to continue search on new
/// data
pub fn full_statement(
    data: &[u8],
    continuation: Option<Continuation>,
) -> Result<usize, Continuation> {
    let mut iter = data.iter().enumerate().peekable();
    if let Some(cont) = continuation.as_ref() {
        if cont.position > 0 {
            iter.nth(cont.position - 1);
        }
    }
    let mut braces_buf = continuation
        .map(|cont| cont.braces)
        .unwrap_or_else(|| Vec::with_capacity(8));
    'outer: while let Some((idx, b)) = iter.next() {
        match b {
            b'"' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'\\' => {
                            // skip any next char, even quote
                            iter.next();
                        }
                        b'"' => continue 'outer,
                        _ => continue,
                    }
                }
                return Err(Continuation {
                    position: idx,
                    braces: braces_buf,
                });
            }
            b'\'' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'\\' => {
                            // skip any next char, even quote
                            iter.next();
                        }
                        b'\'' => continue 'outer,
                        _ => continue,
                    }
                }
                return Err(Continuation {
                    position: idx,
                    braces: braces_buf,
                });
            }
            b'r' => {
                if matches!(iter.peek(), Some((_, b'b'))) {
                    // rb'something' -- skip `b` but match on quote
                    iter.next();
                };
                match iter.peek() {
                    None => {
                        return Err(Continuation {
                            position: idx,
                            braces: braces_buf,
                        });
                    }
                    Some((_, start @ (b'\'' | b'"'))) => {
                        let end = *start;
                        iter.next();
                        for (_, b) in iter.by_ref() {
                            if b == end {
                                continue 'outer;
                            }
                        }
                        return Err(Continuation {
                            position: idx,
                            braces: braces_buf,
                        });
                    }
                    Some((_, _)) => continue,
                }
            }
            b'`' => {
                for (_, b) in iter.by_ref() {
                    match b {
                        b'`' => continue 'outer,
                        _ => continue,
                    }
                }
                return Err(Continuation {
                    position: idx,
                    braces: braces_buf,
                });
            }
            b'#' => {
                for (_, &b) in iter.by_ref() {
                    if b == b'\n' {
                        continue 'outer;
                    }
                }
                return Err(Continuation {
                    position: idx,
                    braces: braces_buf,
                });
            }
            b'$' => {
                match iter.next() {
                    Some((end_idx, b'$')) => {
                        let end = find(&data[end_idx + 1..], b"$$");
                        if let Some(end) = end {
                            iter.nth(end + end_idx - idx);
                            continue 'outer;
                        }
                        return Err(Continuation {
                            position: idx,
                            braces: braces_buf,
                        });
                    }
                    Some((_, b'A'..=b'Z')) | Some((_, b'a'..=b'z')) | Some((_, b'_')) => {}
                    // Not a dollar-quote
                    Some((_, _)) => continue 'outer,
                    None => {
                        return Err(Continuation {
                            position: idx,
                            braces: braces_buf,
                        })
                    }
                }
                loop {
                    let (c_idx, c) = if let Some(pair) = iter.peek() {
                        *pair
                    } else {
                        return Err(Continuation {
                            position: idx,
                            braces: braces_buf,
                        });
                    };
                    match c {
                        b'$' => {
                            let end_idx = c_idx + 1;
                            let marker_size = end_idx - idx;
                            if let Some(end) = find(&data[end_idx..], &data[idx..end_idx]) {
                                iter.nth(1 + end + marker_size - 1);
                                continue 'outer;
                            }
                            return Err(Continuation {
                                position: idx,
                                braces: braces_buf,
                            });
                        }
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' => {}
                        // Not a dollar-quote
                        _ => continue 'outer,
                    }
                    iter.next();
                }
            }
            b'{' => braces_buf.push(b'}'),
            b'(' => braces_buf.push(b')'),
            b'[' => braces_buf.push(b']'),
            b'}' | b')' | b']' if braces_buf.last() == Some(b) => {
                braces_buf.pop();
            }
            b';' if braces_buf.is_empty() => return Ok(idx + 1),
            _ => continue,
        }
    }
    Err(Continuation {
        position: data.len(),
        braces: braces_buf,
    })
}

/// Returns true if the text has no partial statements
///
/// This equivalent to `text.trim().is_empty()` except it also ignores
/// EdgeQL comments.
///
/// This is useful to find out whether last part of text split by
/// `full_statement` contains anything relevant. Before this function we
/// couldn't add a comment at the end of EdgeQL file.
pub fn is_empty(text: &str) -> bool {
    let mut iter = text.chars();
    loop {
        let cur_char = match iter.next() {
            Some(c) => c,
            None => return true,
        };
        match cur_char {
            '\u{feff}' | '\r' | '\t' | '\n' | ' ' | ';' => continue,
            // Comment
            '#' => {
                for c in iter.by_ref() {
                    if c == '\r' || c == '\n' {
                        break;
                    }
                }
                continue;
            }
            _ => return false,
        }
    }
}
