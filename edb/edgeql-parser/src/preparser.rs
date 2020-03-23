use twoway::find_bytes;

#[derive(Debug, PartialEq)]
pub struct Continuation {
    position: usize,
    braces: Vec<u8>,
}

/// Returns index of semicolon, or position where to continue search on new
/// data
pub fn full_statement(data: &[u8], continuation: Option<Continuation>)
    -> Result<usize, Continuation>
{
    let mut iter = data.iter().enumerate().peekable();
    continuation.as_ref().map(|cont| {
        if cont.position > 0 {
            iter.nth(cont.position-1);
        }
    });
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
                return Err(Continuation { position: idx, braces: braces_buf });
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
                return Err(Continuation { position: idx, braces: braces_buf });
            }
            b'`' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'`' => continue 'outer,
                        _ => continue,
                    }
                }
                return Err(Continuation { position: idx, braces: braces_buf });
            }
            b'#' => {
                while let Some((_, &b)) = iter.next() {
                    if b == b'\n' {
                        continue 'outer;
                    }
                }
                return Err(Continuation { position: idx, braces: braces_buf });
            }
            b'$' => {
                match iter.next() {
                    Some((end_idx, b'$')) => {
                        if let Some(end) = find_bytes(&data[end_idx+1..],
                                                      b"$$")
                        {
                            iter.nth(end + end_idx - idx);
                            continue 'outer;
                        }
                        return Err(Continuation {
                            position: idx,
                            braces: braces_buf,
                        });
                    }
                    | Some((_, b'A'..=b'Z'))
                    | Some((_, b'a'..=b'z'))
                    | Some((_, b'_'))
                    => { }
                    // Not a dollar-quote
                    Some((_, _)) => continue 'outer,
                    None => return Err(Continuation {
                        position: idx,
                        braces: braces_buf,
                    }),
                }
                loop {
                    match iter.next() {
                        Some((end_idx, b'$')) => {
                            if let Some(end) = find_bytes(&data[end_idx+1..],
                                                          &data[idx..end_idx+1])
                            {
                                iter.nth(end + end_idx - idx);
                                continue 'outer;
                            }
                            return Err(Continuation {
                                position: idx,
                                braces: braces_buf,
                            });
                        }
                        | Some((_, b'A'..=b'Z'))
                        | Some((_, b'a'..=b'z'))
                        | Some((_, b'0'..=b'9'))
                        | Some((_, b'_'))
                        => continue,
                        // Not a dollar-quote
                        Some((_, _)) => continue 'outer,
                        None => return Err(Continuation {
                            position: idx,
                            braces: braces_buf,
                        }),
                    }
                }
            }
            b'{' => braces_buf.push(b'}'),
            b'(' => braces_buf.push(b')'),
            b'[' => braces_buf.push(b']'),
            b'}' | b')' | b']'
            if braces_buf.last() == Some(b)
            => { braces_buf.pop(); }
            b';' if braces_buf.len() == 0 => return Ok(idx+1),
            _ => continue,
        }
    }
    return Err(Continuation { position: data.len(), braces: braces_buf });
}
