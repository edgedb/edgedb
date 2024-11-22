pub fn unquote_bytes(value: &str) -> Result<Vec<u8>, String> {
    let idx = value
        .find(['\'', '"'])
        .ok_or_else(|| "invalid bytes literal: missing quotes".to_string())?;
    let prefix = &value[..idx];
    match prefix {
        "br" | "rb" => Ok(value[3..value.len() - 1].as_bytes().to_vec()),
        "b" => Ok(unquote_bytes_inner(&value[2..value.len() - 1])?),
        _ => {
            return Err(format_args!(
                "prefix {:?} is not allowed for bytes, allowed: `b`, `rb`",
                prefix
            )
            .to_string())
        }
    }
}

fn unquote_bytes_inner(s: &str) -> Result<Vec<u8>, String> {
    let mut res = Vec::with_capacity(s.len());
    let mut bytes = s.as_bytes().iter();
    while let Some(&c) = bytes.next() {
        match c {
            b'\\' => {
                match *bytes.next().expect("slash cant be at the end") {
                    c @ b'"' | c @ b'\\' | c @ b'/' | c @ b'\'' => res.push(c),
                    b'b' => res.push(b'\x08'),
                    b'f' => res.push(b'\x0C'),
                    b'n' => res.push(b'\n'),
                    b'r' => res.push(b'\r'),
                    b't' => res.push(b'\t'),
                    b'x' => {
                        let tail = &s[s.len() - bytes.as_slice().len()..];
                        let hex = tail.get(0..2);
                        let code = hex
                            .and_then(|s| u8::from_str_radix(s, 16).ok())
                            .ok_or_else(|| {
                                format!(
                                    "invalid bytes literal: \
                                invalid escape sequence '\\x{}'",
                                    hex.unwrap_or(tail).escape_debug()
                                )
                            })?;
                        res.push(code);
                        bytes.nth(1);
                    }
                    b'\r' | b'\n' => {
                        let nskip = bytes
                            .as_slice()
                            .iter()
                            .take_while(|&&x| x.is_ascii_whitespace())
                            .count();
                        if nskip > 0 {
                            bytes.nth(nskip - 1);
                        }
                    }
                    c => {
                        let ch = if c < 0x7f {
                            c as char
                        } else {
                            // recover the unicode byte
                            s[s.len() - bytes.as_slice().len() - 1..]
                                .chars()
                                .next()
                                .unwrap()
                        };
                        return Err(format!(
                            "invalid bytes literal: \
                            invalid escape sequence '\\{}'",
                            ch.escape_debug()
                        ));
                    }
                }
            }
            c => res.push(c),
        }
    }

    Ok(res)
}

#[test]
fn simple_bytes() {
    assert_eq!(unquote_bytes_inner(r"\x09").unwrap(), b"\x09");
    assert_eq!(unquote_bytes_inner(r"\x0A").unwrap(), b"\x0A");
    assert_eq!(unquote_bytes_inner(r"\x0D").unwrap(), b"\x0D");
    assert_eq!(unquote_bytes_inner(r"\x20").unwrap(), b"\x20");
    assert_eq!(unquote_bytes(r"b'\x09'").unwrap(), b"\x09");
    assert_eq!(unquote_bytes(r"b'\x0A'").unwrap(), b"\x0A");
    assert_eq!(unquote_bytes(r"b'\x0D'").unwrap(), b"\x0D");
    assert_eq!(unquote_bytes(r"b'\x20'").unwrap(), b"\x20");
    assert_eq!(unquote_bytes(r"br'\x09'").unwrap(), b"\\x09");
    assert_eq!(unquote_bytes(r"br'\x0A'").unwrap(), b"\\x0A");
    assert_eq!(unquote_bytes(r"br'\x0D'").unwrap(), b"\\x0D");
    assert_eq!(unquote_bytes(r"br'\x20'").unwrap(), b"\\x20");
}

#[test]
fn newline_escaping_bytes() {
    assert_eq!(
        unquote_bytes_inner(
            r"hello \
                                world"
        )
        .unwrap(),
        b"hello world"
    );
    assert_eq!(
        unquote_bytes(
            r"br'hello \
                                world'"
        )
        .unwrap(),
        b"hello \\\n                                world"
    );

    assert_eq!(
        unquote_bytes_inner(
            r"bb\
aa \
            bb"
        )
        .unwrap(),
        b"bbaa bb"
    );
    assert_eq!(
        unquote_bytes(
            r"rb'bb\
aa \
            bb'"
        )
        .unwrap(),
        b"bb\\\naa \\\n            bb"
    );
    assert_eq!(
        unquote_bytes_inner(
            r"bb\

        aa"
        )
        .unwrap(),
        b"bbaa"
    );
    assert_eq!(
        unquote_bytes(
            r"br'bb\

        aa'"
        )
        .unwrap(),
        b"bb\\\n\n        aa"
    );
    assert_eq!(
        unquote_bytes_inner(
            r"bb\
        \
        aa"
        )
        .unwrap(),
        b"bbaa"
    );
    assert_eq!(
        unquote_bytes(
            r"rb'bb\
        \
        aa'"
        )
        .unwrap(),
        b"bb\\\n        \\\n        aa"
    );
    assert_eq!(unquote_bytes_inner("bb\\\r   aa").unwrap(), b"bbaa");
    assert_eq!(unquote_bytes("br'bb\\\r   aa'").unwrap(), b"bb\\\r   aa");
    assert_eq!(unquote_bytes_inner("bb\\\r\n   aa").unwrap(), b"bbaa");
    assert_eq!(
        unquote_bytes("rb'bb\\\r\n   aa'").unwrap(),
        b"bb\\\r\n   aa"
    );
}

#[test]
fn complex_bytes() {
    assert_eq!(
        unquote_bytes_inner(r"\x09 hello \x0A there").unwrap(),
        b"\x09 hello \x0A there"
    );
    assert_eq!(
        unquote_bytes(r"br'\x09 hello \x0A there'").unwrap(),
        b"\\x09 hello \\x0A there"
    );
}
