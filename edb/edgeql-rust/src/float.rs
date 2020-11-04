use std::borrow::Cow;


pub fn convert(text: &str) -> Result<f64, Cow<'static, str>> {
    let value = text.replace("_", "").parse()
        .map_err(|e| format!("can't parse std::float64: {}", e))?;
    if value == f64::INFINITY || value == -f64::INFINITY {
        return Err("number is out of range for std::float64".into());
    }
    if value == 0.0 {
        let mend = text.find(|c| c == 'e' || c == 'E')
            .unwrap_or(text.len());
        let mantissa = &text[..mend];
        if mantissa.chars().any(|c| c != '0' && c != '.') {
            return Err("number is out of range for std::float64".into());
        }
    }
    Ok(value)
}
