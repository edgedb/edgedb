use pgrust::connection::dsn::{
    parse_postgres_dsn, parse_postgres_dsn_env, RawConnectionParameters,
};
use std::collections::HashMap;

mod dsn_libpq;
macro_rules! assert_eq_map {
    ($left:expr, $right:expr $(, $($arg:tt)*)?) => {{
        fn make_string(s: impl AsRef<str>) -> String {
            s.as_ref().to_string()
        }
        let left: HashMap<_, _> = $left.clone().into();
        let right: HashMap<_, _> = $right.clone().into();
        let left: std::collections::BTreeMap<String, String> = left
            .into_iter()
            .map(|(k, v)| (make_string(k), make_string(v)))
            .collect();
        let right: std::collections::BTreeMap<String, String> = right
            .into_iter()
            .map(|(k, v)| (make_string(k), make_string(v)))
            .collect();

        pretty_assertions::assert_eq!(left, right $(, $($arg)*)?);
    }};
}

#[track_caller]
pub(crate) fn test(
    dsn: &str,
    expected: HashMap<String, String>,
    env: HashMap<String, String>,
    expect_mismatch: bool,
    no_env: bool,
) {
    eprintln!("DSN: {dsn:?}");

    let mut ours_no_env = match parse_postgres_dsn(dsn) {
        Err(res) => panic!("Expected test to pass {dsn:?}, but instead failed:\n{res:#?}"),
        Ok(res) => res,
    };

    eprintln!("Parsed: {ours_no_env:#?}");
    let ours: HashMap<String, String> = ours_no_env.clone().into();
    eprintln!("Parsed (map): {ours:#?}");

    let url = ours_no_env.to_url();
    let roundtrip = match parse_postgres_dsn(&url) {
        Err(res) => {
            panic!("Expected roundtripped URL to pass {url:?}, but instead failed:\n{res:#?}")
        }
        Ok(res) => res,
    };
    assert_eq_map!(
        roundtrip,
        ours_no_env,
        "Did not maintain fidelity through the roundtrip! ({url:?})"
    );

    if no_env {
        assert_eq_map!(
            expected,
            ours_no_env,
            "crate mismatch from expected when parsing {dsn:?}"
        );
    } else {
        let ours = match parse_postgres_dsn_env(dsn, env) {
            Err(res) => panic!("Expected test to pass {dsn:?}, but instead failed:\n{res:#?}"),
            Ok(res) => res,
        };

        // Avoid the hassle of specifying the default SSL mode unless explicitly tested for.
        let mut ours: HashMap<String, String> = RawConnectionParameters::from(ours).into();
        if !expected.contains_key("sslmode") {
            ours.remove("sslmode");
        }

        assert_eq_map!(
            expected,
            ours,
            "crate mismatch from expected when parsing {dsn:?}"
        );
    }

    let res = dsn_libpq::pq_conn_parse_non_defaults(dsn);
    eprintln!("{res:?}");
    if expect_mismatch {
        assert!(res.is_err());
    } else {
        let libpq = match res {
            Err(res) => panic!("Expected test to pass {dsn:?}, but instead failed:\n{res:#?}"),
            Ok(res) => res,
        };

        // Only compare for no_env
        if no_env {
            assert_eq_map!(
                libpq,
                expected,
                "libpq mismatch from expected when parsing {dsn:?}"
            );
        } else {
            // We cannot detect libpq's defaults here so we just remove them
            // from the test
            if ours_no_env.port == Some(vec![Some(5432)]) {
                ours_no_env.port = None
            }
            assert_eq_map!(
                libpq,
                ours_no_env,
                "libpq mismatch from expected when parsing {dsn:?}"
            );
        }
    }
}

#[track_caller]
pub(crate) fn test_fail(
    dsn: &str,
    env: HashMap<String, String>,
    expect_mismatch: bool,
    no_env: bool,
) {
    let res = dsn_libpq::pq_conn_parse_non_defaults(dsn);
    eprintln!("libpq: {res:#?}");
    if expect_mismatch {
        assert!(res.is_ok());
    } else if let Ok(res) = res {
        panic!("Expected test to fail {dsn:?}, but instead parsed correctly:\n{res:#?}")
    }
    if no_env {
        match parse_postgres_dsn(dsn) {
            Ok(res) => {
                panic!("Expected test to fail {dsn:?}, but instead parsed correctly:\n{res:#?}")
            }
            Err(e) => {
                eprintln!("Error: {e:#?}")
            }
        }
    } else {
        match parse_postgres_dsn_env(dsn, env) {
            Ok(res) => {
                panic!("Expected test to fail {dsn:?}, but instead parsed correctly:\n{res:#?}")
            }
            Err(e) => {
                eprintln!("Error: {e:#?}")
            }
        }
    }
}

#[macro_export]
macro_rules! env {
    ({ $($key:literal : $value:expr),* $(,)? }) => {{
        #[allow(unused_mut)]
        let mut map = std::collections::HashMap::new();
        $(
            map.insert($key.to_string(), $value.to_string());
        )*
        map
    }};
    () => {
        std::collections::HashMap::new()
    };
}
pub use env;

#[macro_export]
macro_rules! test_case {
    ($name:ident, $urn:literal, output=$output:tt $( , expect_libpq_mismatch=$reason:literal )? $( , no_env=$no_env:ident )?) => {
        paste::paste!( #[test] fn [< test_ $name >]() {
            let expect_libpq_mismatch: &[&'static str] = &[$($reason)?];
            let no_env: &[&'static str] = &[$(stringify!($no_env))?];
            $crate::test_util::test($urn, $crate::test_util::env!($output), $crate::test_util::env!({}), expect_libpq_mismatch.len() > 0, no_env.len() > 0)
        } );
    };
    ($name:ident, $urn:literal, output=$output:tt, extra=$extra:tt $( , expect_libpq_mismatch=$reason:literal )? $( , no_env=$no_env:ident )?) => {
        paste::paste!( #[test] fn [< test_ $name >]() {
            let expect_libpq_mismatch: &[&'static str] = &[$($reason)?];
            let no_env: &[&'static str] = &[$(stringify!($no_env))?];
            $crate::test_util::test($urn, $crate::test_util::env!($output),$crate::test_util::env!({}), expect_libpq_mismatch.len() > 0, no_env.len() > 0)
        } );
    };
    ($name:ident, $urn:literal, error=$error:tt $( , expect_libpq_mismatch=$reason:literal )? $( , no_env=$no_env:ident )?) => {
        paste::paste!( #[test] fn [< test_ $name >]() {
            let expect_libpq_mismatch: &[&'static str] = &[$($reason)?];
            let no_env: &[&'static str] = &[$(stringify!($no_env))?];
            $crate::test_util::test_fail($urn, $crate::test_util::env!({}), expect_libpq_mismatch.len() > 0, no_env.len() > 0)
        } );
    };
    ($name:ident, $urn:literal, env=$env:tt, output=$output:tt $( , expect_libpq_mismatch=$reason:literal )? $( , no_env=$no_env:ident )?) => {
        paste::paste!( #[test] fn [< test_ $name >]() {
            let expect_libpq_mismatch: &[&'static str] = &[$($reason)?];
            let no_env: &[&'static str] = &[$(stringify!($no_env))?];
            $crate::test_util::test($urn, $crate::test_util::env!($output), $crate::test_util::env!($env), expect_libpq_mismatch.len() > 0, no_env.len() > 0)
        } );
    };
    ($name:ident, $urn:literal, env=$env:tt, error=$output:tt $( , expect_libpq_mismatch=$reason:literal )? $( , no_env=$no_env:ident )?) => {
        paste::paste!( #[test] fn [< test_ $name >]() {
            let expect_libpq_mismatch: &[&'static str] = &[$($reason)?];
            let no_env: &[&'static str] = &[$(stringify!($no_env))?];
            $crate::test_util::test_fail($urn, $crate::test_util::env!($env), expect_libpq_mismatch.len() > 0, no_env.len() > 0)
        } );
    };
}
