use pgrust::parse_postgres_url;

fn main() {
    let dsn = std::env::args().nth(1).expect("No DSN provided");

    let mut params = parse_postgres_url(&dsn, std::env::vars()).unwrap();
    #[allow(deprecated)]
    let home = std::env::home_dir().unwrap();
    eprintln!("DSN: {dsn}\n----\n{:#?}\n", params);
    params
        .password
        .resolve(&home, &params.hosts, &params.database, &params.user)
        .unwrap();
    eprintln!(
        "Resolved password:\n------------------\n{:#?}\n",
        params.password
    );
    let ssl = params.ssl.resolve(&home).unwrap();
    eprintln!("Resolved SSL:\n-------------\n{:#?}\n", ssl);
}
