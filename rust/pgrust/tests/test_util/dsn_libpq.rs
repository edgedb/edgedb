use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ptr;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct _PQconninfoOption {
    /// The keyword of the option
    keyword: *mut libc::c_char,
    /// Fallback environment variable name
    envvar: *mut libc::c_char,
    /// Fallback compiled in default value
    compiled: *mut libc::c_char,
    /// Option's current value, or NULL
    val: *mut libc::c_char,
    /// Label for field in connect dialog
    label: *mut libc::c_char,
    /// Indicates how to display this field in a connect dialog.
    /// Values are:
    /// - "": Display entered value as is
    /// - "*": Password field - hide value
    /// - "D": Debug option - don't show by default
    dispchar: *mut libc::c_char,
    /// Field size in characters for dialog
    dispsize: libc::c_int,
}

/// Rust-friendly version of PQconninfoOption
#[derive(Debug, Clone)]
#[allow(unused)]
pub struct PQConnInfoOption {
    /// The keyword of the option
    pub keyword: Option<String>,
    /// Fallback environment variable name
    pub envvar: Option<String>,
    /// Fallback compiled in default value
    pub compiled: Option<String>,
    /// Option's current value, or None
    pub val: Option<String>,
    /// Label for field in connect dialog
    pub label: Option<String>,
    /// Indicates how to display this field in a connect dialog.
    /// Values are:
    /// - "": Display entered value as is
    /// - "*": Password field - hide value
    /// - "D": Debug option - don't show by default
    pub dispchar: Option<String>,
    /// Field size in characters for dialog
    pub dispsize: i32,
}

impl From<&_PQconninfoOption> for PQConnInfoOption {
    fn from(option: &_PQconninfoOption) -> Self {
        unsafe {
            PQConnInfoOption {
                keyword: (!option.keyword.is_null()).then(|| {
                    CStr::from_ptr(option.keyword)
                        .to_string_lossy()
                        .into_owned()
                }),
                envvar: (!option.envvar.is_null())
                    .then(|| CStr::from_ptr(option.envvar).to_string_lossy().into_owned()),
                compiled: (!option.compiled.is_null()).then(|| {
                    CStr::from_ptr(option.compiled)
                        .to_string_lossy()
                        .into_owned()
                }),
                val: (!option.val.is_null())
                    .then(|| CStr::from_ptr(option.val).to_string_lossy().into_owned()),
                label: (!option.label.is_null())
                    .then(|| CStr::from_ptr(option.label).to_string_lossy().into_owned()),
                dispchar: (!option.dispchar.is_null()).then(|| {
                    CStr::from_ptr(option.dispchar)
                        .to_string_lossy()
                        .into_owned()
                }),
                dispsize: option.dispsize,
            }
        }
    }
}

#[link(name = "pq")]
extern "C" {
    /// Parses a connection string and returns the resulting connection options.
    ///
    /// This function parses a string in the same way as `PQconnectdb()` would,
    /// and returns an array of connection options. If parsing fails, it returns NULL.
    /// The returned options only include those explicitly specified in the string,
    /// not any default values.
    ///
    /// # Arguments
    ///
    /// * `conninfo` - The connection string to parse.
    /// * `errmsg` - If not NULL, it will be set to NULL on success, or a malloc'd
    ///   error message string on failure. The caller should free this string
    ///   using `PQfreemem()` when it's no longer needed.
    ///
    /// # Returns
    ///
    /// A pointer to a dynamically allocated array of `PQconninfoOption` structures,
    /// or NULL on failure. In out-of-memory conditions, both `*errmsg` and the
    /// return value could be NULL.
    ///
    /// # Safety
    ///
    /// The returned array should be freed using `PQconninfoFree()` when no longer needed.
    fn PQconninfoParse(
        conninfo: *const libc::c_char,
        errmsg: *mut *mut libc::c_char,
    ) -> *mut _PQconninfoOption;

    /// Constructs a default connection options array.
    ///
    /// This function identifies all available options and shows any default values
    /// that are available from the environment, etc. On error (e.g., out of memory),
    /// NULL is returned.
    ///
    /// Using this function, an application may determine all possible options
    /// and their current default values.
    ///
    /// # Returns
    ///
    /// A pointer to a dynamically allocated array of `PQconninfoOption` structures,
    /// or NULL on failure.
    ///
    /// # Safety
    ///
    /// The returned array should be freed using `PQconninfoFree()` when no longer needed.
    ///
    /// # Note
    ///
    /// As of PostgreSQL 7.0, the returned array is dynamically allocated.
    /// Pre-7.0 applications that use this function will see a small memory leak
    /// until they are updated to call `PQconninfoFree()`.
    fn PQconndefaults() -> *mut _PQconninfoOption;

    /// Frees the data structure returned by `PQconndefaults()` or `PQconninfoParse()`.
    ///
    /// This function should be used to free the memory allocated by `PQconndefaults()`
    /// or `PQconninfoParse()` when it's no longer needed.
    ///
    /// # Arguments
    ///
    /// * `connOptions` - A pointer to the `PQconninfoOption` structure to be freed.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it operates on raw pointers. The caller must
    /// ensure that the pointer is valid and points to a structure allocated by
    /// `PQconndefaults()` or `PQconninfoParse()`.
    fn PQconninfoFree(connOptions: *mut _PQconninfoOption);
}

fn parse_conninfo_options(options: *mut _PQconninfoOption) -> Vec<PQConnInfoOption> {
    let mut result = Vec::new();
    let mut current = options;

    while !current.is_null() && unsafe { !(*current).keyword.is_null() } {
        let option = unsafe { &*current };
        result.push(PQConnInfoOption::from(option));
        current = unsafe { current.add(1) };
    }

    result
}

/// Parses a connection string and returns the resulting connection options.
///
/// # Arguments
///
/// * `conninfo` - The connection string to parse.
///
/// # Returns
///
/// A `Result` containing a `Vec` of `PQConnInfoOption` on success, or an error message on failure.
pub fn pq_conninfo_parse(conninfo: &str) -> Result<Vec<PQConnInfoOption>, String> {
    let c_conninfo = CString::new(conninfo).map_err(|e| e.to_string())?;
    let mut errmsg: *mut libc::c_char = ptr::null_mut();

    let options = unsafe { PQconninfoParse(c_conninfo.as_ptr(), &mut errmsg) };

    if options.is_null() {
        let error = if errmsg.is_null() {
            "Unknown error occurred during parsing connection info".to_string()
        } else {
            let error_str = unsafe { CStr::from_ptr(errmsg) }
                .to_string_lossy()
                .into_owned();
            unsafe { libc::free(errmsg as *mut libc::c_void) };
            error_str
        };
        return Err(error);
    }

    let result = parse_conninfo_options(options);
    unsafe { PQconninfoFree(options) };
    Ok(result)
}

/// Constructs a default connection options array.
///
/// # Returns
///
/// A `Result` containing a `Vec` of `PQConnInfoOption` on success, or an error message on failure.
pub fn pq_conn_defaults() -> Result<Vec<PQConnInfoOption>, String> {
    let options = unsafe { PQconndefaults() };

    if options.is_null() {
        return Err("Failed to get default connection options".to_string());
    }

    let result = parse_conninfo_options(options);
    unsafe { PQconninfoFree(options) };
    Ok(result)
}

pub fn pq_conn_parse_non_defaults(urn: &str) -> Result<HashMap<String, String>, String> {
    // Parse the given URN
    let parsed_options = pq_conninfo_parse(urn)?;

    // Get the default options
    let default_options = pq_conn_defaults()?;

    // Create a HashMap to store non-default values
    let mut non_defaults = HashMap::new();

    // Compare parsed options with defaults and store non-default values
    for parsed in parsed_options {
        if let Some(keyword) = &parsed.keyword {
            if let Some(val) = &parsed.val {
                // Find the corresponding default option
                if let Some(default) = default_options
                    .iter()
                    .find(|&d| d.keyword.as_ref() == Some(keyword))
                {
                    // If the value is different from the default, add it to non_defaults
                    if default.val.as_ref() != Some(val) {
                        non_defaults.insert(keyword.clone(), val.clone());
                    }
                } else {
                    // If there's no corresponding default, it's a non-default value
                    non_defaults.insert(keyword.clone(), val.clone());
                }
            }
        }
    }

    Ok(non_defaults)
}

#[test]
fn test() {
    std::env::set_var("PGUSER", "matt");
    eprintln!("{:#?}", pq_conn_parse_non_defaults("postgres://foo@/"));
}
